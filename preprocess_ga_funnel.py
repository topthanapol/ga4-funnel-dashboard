"""
GA4 Funnel Analysis Pre-processor
Queries Databricks directly, outputs summarized JSON for the dashboard.
"""
from databricks import sql as databricks_sql
import pandas as pd
import json
import os
import time

# Databricks connection
DATABRICKS_HOST = "886789292378781.1.gcp.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/fb9b52635d49e79e"
DATABRICKS_TABLE = "main.ga360_events.ga_traffic_events"
DATE_FROM = "2026-03-20"

OUTPUT_PATH = "data/ga_funnel_data.json"

# Funnel step definitions (order matters)
FUNNEL_EVENTS = [
    "view_item_list",
    "add_to_cart",
    "view_cart",
    "begin_checkout",
    "add_payment_info",
    "purchase",
]
FUNNEL_LABELS = [
    "View Items",
    "Add to Cart",
    "View Cart",
    "Begin Checkout",
    "Add Payment",
    "Purchase",
]

# Source grouping rules (references 'source' alias from subquery)
SOURCE_CASE = """
    CASE
        WHEN source = '(direct)' OR source IS NULL OR source = '' THEN 'Direct'
        WHEN source ILIKE 'richmenu%' THEN 'LINE Richmenu'
        WHEN source ILIKE '%google%' OR source ILIKE '%bing%' THEN 'Search'
        WHEN source ILIKE '%facebook%' OR source ILIKE '%tiktok%'
             OR source ILIKE '%line%' AND source NOT ILIKE 'richmenu%' THEN 'Social'
        ELSE 'Others'
    END
"""


def main():
    start = time.time()

    print("[0/42] Connecting to Databricks...")
    conn = databricks_sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        auth_type="databricks-cli",
    )
    cursor = conn.cursor()

    def query(sql):
        cursor.execute(sql)
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=cols)

    # Create a temp view for reuse
    print("[1/42] Creating temp view...")
    cursor.execute(f"""
        CREATE OR REPLACE TEMP VIEW events AS
        SELECT *,
            {SOURCE_CASE} AS source_group
        FROM (
            SELECT
                CAST(p_event_date AS DATE) AS event_date,
                event_ts_th AS event_ts,
                HOUR(event_ts_th) AS event_hour,
                DAYOFWEEK(CAST(p_event_date AS DATE)) AS day_of_week,
                user_id_final AS uid,
                event_name,
                traffic_source_source AS source,
                platform,
                COALESCE(ec_purchase_revenue, 0) AS revenue,
                ec_transaction_id AS transaction_id,
                COALESCE(engagement_time_msec, 0) AS engagement_msec,
                ga_session_number AS session_number,
                first_item_name,
                device_category,
                os,
                browser_final,
                geo_region,
                page_location
            FROM {DATABRICKS_TABLE}
            WHERE user_id_final LIKE 'U%'
              AND p_event_date >= '{DATE_FROM}'
        ) raw
    """)

    # --- Query 1: Meta ---
    print("[2/42] Meta stats...")
    meta = query("""
        SELECT
            COUNT(*) AS total_events,
            COUNT(DISTINCT uid) AS total_users,
            MIN(event_date) AS date_from,
            MAX(event_date) AS date_to,
            COUNT(DISTINCT event_date) AS num_days
        FROM events
    """).to_dict(orient="records")[0]
    meta["date_from"] = str(meta["date_from"])[:10]
    meta["date_to"] = str(meta["date_to"])[:10]
    print(f"   Total events: {meta['total_events']:,}, Users: {meta['total_users']:,}")

    # --- Query 2: KPI ---
    print("[3/42] KPI calculations...")
    kpi = query("""
        WITH purchase_events AS (
            SELECT uid, event_ts,
                ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS purchase_seq
            FROM events
            WHERE event_name = 'purchase'
        ),
        first_purchase AS (
            SELECT uid, event_ts AS first_purchase_ts
            FROM purchase_events WHERE purchase_seq = 1
        ),
        second_purchase AS (
            SELECT uid, event_ts AS second_purchase_ts
            FROM purchase_events WHERE purchase_seq = 2
        ),
        user_first AS (
            SELECT uid, MIN(event_ts) AS first_touch
            FROM events
            GROUP BY uid
        ),
        user_events AS (
            SELECT uid, COUNT(*) AS event_count
            FROM events
            GROUP BY uid
        ),
        purchaser_counts AS (
            SELECT uid, COUNT(*) AS purchase_count
            FROM events WHERE event_name = 'purchase'
            GROUP BY uid
        )
        SELECT
            (SELECT COUNT(DISTINCT uid) FROM events WHERE event_name = 'purchase') AS purchasers,
            (SELECT COUNT(DISTINCT uid) FROM events) AS total_users,
            (SELECT ROUND(AVG(event_count), 1) FROM user_events) AS avg_events_per_user,
            (SELECT ROUND(AVG((unix_timestamp(fp.first_purchase_ts) - unix_timestamp(uf.first_touch)) / 3600.0), 1)
             FROM first_purchase fp JOIN user_first uf ON fp.uid = uf.uid
             WHERE uf.first_touch < fp.first_purchase_ts) AS avg_hours_to_first_purchase,
            (SELECT ROUND(AVG((unix_timestamp(sp.second_purchase_ts) - unix_timestamp(fp.first_purchase_ts)) / 3600.0), 1)
             FROM first_purchase fp JOIN second_purchase sp ON fp.uid = sp.uid) AS avg_hours_to_repeat,
            (SELECT ROUND(100.0 * COUNT(CASE WHEN purchase_count > 1 THEN 1 END) / NULLIF(COUNT(*), 0), 1)
             FROM purchaser_counts) AS repeat_purchase_pct
    """).to_dict(orient="records")[0]

    kpi_data = {
        "total_users": int(meta["total_users"]),
        "conversion_rate": round(100.0 * int(kpi["purchasers"]) / int(meta["total_users"]), 1),
        "purchases": int(kpi["purchasers"]),
        "avg_events_per_user": float(kpi["avg_events_per_user"]),
        "avg_hours_to_first_purchase": float(kpi["avg_hours_to_first_purchase"]) if kpi["avg_hours_to_first_purchase"] else 0,
        "avg_hours_to_repeat": float(kpi["avg_hours_to_repeat"]) if kpi["avg_hours_to_repeat"] else 0,
        "repeat_purchase_pct": float(kpi["repeat_purchase_pct"]) if kpi["repeat_purchase_pct"] else 0,
    }
    print(f"   Conv rate: {kpi_data['conversion_rate']}%, Purchasers: {kpi_data['purchases']:,}")

    # --- Query 3: Funnel ---
    print("[4/42] Funnel counts...")
    funnel_parts = []
    for evt in FUNNEL_EVENTS:
        funnel_parts.append(
            f"COUNT(DISTINCT CASE WHEN event_name = '{evt}' THEN uid END) AS `{evt}`"
        )
    funnel_sql = f"SELECT {', '.join(funnel_parts)} FROM events"
    funnel_raw = query(funnel_sql).to_dict(orient="records")[0]

    funnel_data = []
    total = int(meta["total_users"])
    for i, evt in enumerate(FUNNEL_EVENTS):
        count = int(funnel_raw[evt])
        funnel_data.append({
            "step": FUNNEL_LABELS[i],
            "event": evt,
            "users": count,
            "pct_of_total": round(100.0 * count / total, 1),
            "drop_off_pct": round(100.0 * (1 - count / (funnel_data[-1]["users"] if i > 0 else total)), 1) if i > 0 else 0,
        })
    # Prepend total users step
    funnel_data.insert(0, {
        "step": "Total Users",
        "event": "all",
        "users": total,
        "pct_of_total": 100.0,
        "drop_off_pct": 0,
    })

    # --- Query 4: Sankey (layered transitions) ---
    print("[5/42] Sankey transitions...")
    sankey_sql = """
        WITH funnel_events AS (
            SELECT uid, event_name, event_ts,
                CASE event_name
                    WHEN 'view_item_list' THEN 1
                    WHEN 'add_to_cart' THEN 2
                    WHEN 'view_cart' THEN 3
                    WHEN 'begin_checkout' THEN 4
                    WHEN 'add_payment_info' THEN 5
                    WHEN 'purchase' THEN 6
                END AS step_num
            FROM events
            WHERE event_name IN ('view_item_list','add_to_cart','view_cart',
                                  'begin_checkout','add_payment_info','purchase')
        ),
        with_next AS (
            SELECT uid, event_name, step_num,
                LEAD(event_name) OVER (PARTITION BY uid ORDER BY event_ts, step_num) AS next_event,
                LEAD(step_num) OVER (PARTITION BY uid ORDER BY event_ts, step_num) AS next_step
            FROM funnel_events
        )
        SELECT
            event_name AS source_event,
            next_event AS target_event,
            COUNT(DISTINCT uid) AS user_count
        FROM with_next
        WHERE next_event IS NOT NULL
          AND next_step >= step_num
        GROUP BY event_name, next_event
        ORDER BY user_count DESC
        LIMIT 30
    """
    sankey_raw = query(sankey_sql)

    event_label_map = dict(zip(FUNNEL_EVENTS, FUNNEL_LABELS))
    sankey_nodes = []
    sankey_links = []
    node_set = set()
    for _, row in sankey_raw.iterrows():
        src = event_label_map.get(row["source_event"], row["source_event"])
        tgt = event_label_map.get(row["target_event"], row["target_event"])
        src_step = FUNNEL_EVENTS.index(row["source_event"]) + 1 if row["source_event"] in FUNNEL_EVENTS else 0
        tgt_step = FUNNEL_EVENTS.index(row["target_event"]) + 1 if row["target_event"] in FUNNEL_EVENTS else 0
        src_label = f"S{src_step}: {src}"
        tgt_label = f"S{tgt_step}: {tgt}"
        if src_label == tgt_label:
            continue
        for n in [src_label, tgt_label]:
            if n not in node_set:
                node_set.add(n)
                sankey_nodes.append({"name": n})
        sankey_links.append({
            "source": src_label,
            "target": tgt_label,
            "value": int(row["user_count"]),
        })

    # --- Query 5: Top User Journeys ---
    print("[6/42] Top user journeys...")
    journeys_sql = """
        WITH funnel_events AS (
            SELECT uid, event_name, event_ts,
                CASE event_name
                    WHEN 'view_item_list' THEN 1
                    WHEN 'add_to_cart' THEN 2
                    WHEN 'view_cart' THEN 3
                    WHEN 'begin_checkout' THEN 4
                    WHEN 'add_payment_info' THEN 5
                    WHEN 'purchase' THEN 6
                END AS step_num
            FROM events
            WHERE event_name IN ('view_item_list','add_to_cart','view_cart',
                                  'begin_checkout','add_payment_info','purchase')
        ),
        deduped AS (
            SELECT uid, event_name, event_ts, step_num,
                LAG(event_name) OVER (PARTITION BY uid ORDER BY event_ts, step_num) AS prev_event
            FROM funnel_events
        ),
        no_consec AS (
            SELECT * FROM deduped
            WHERE prev_event IS NULL OR event_name != prev_event
        ),
        numbered AS (
            SELECT uid, event_name, event_ts, step_num,
                ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts, step_num) AS rn
            FROM no_consec
        ),
        filtered AS (
            SELECT * FROM numbered WHERE rn <= 10
        ),
        paths AS (
            SELECT uid,
                array_join(
                    transform(
                        sort_array(collect_list(struct(event_ts, step_num, event_name))),
                        x -> x.event_name
                    ),
                    ' → '
                ) AS path,
                COUNT(*) AS path_len
            FROM filtered
            GROUP BY uid
        )
        SELECT path, COUNT(*) AS user_count, path_len
        FROM paths
        GROUP BY path, path_len
        ORDER BY user_count DESC
        LIMIT 20
    """
    journeys_raw = query(journeys_sql)
    journeys_data = []
    for _, row in journeys_raw.iterrows():
        steps = row["path"].split(" → ")
        labeled_steps = [event_label_map.get(s, s) for s in steps]
        journeys_data.append({
            "path": labeled_steps,
            "users": int(row["user_count"]),
            "steps": int(row["path_len"]),
        })

    # --- Query 6: Funnel by Source ---
    print("[7/42] Funnel by source...")
    source_parts = []
    for evt in FUNNEL_EVENTS:
        source_parts.append(
            f"COUNT(DISTINCT CASE WHEN event_name = '{evt}' THEN uid END) AS `{evt}`"
        )
    source_sql = f"""
        SELECT
            source_group,
            COUNT(DISTINCT uid) AS total_users,
            {', '.join(source_parts)}
        FROM events
        GROUP BY source_group
        ORDER BY total_users DESC
    """
    source_raw = query(source_sql)
    source_data = []
    for _, row in source_raw.iterrows():
        entry = {"source": row["source_group"], "total_users": int(row["total_users"])}
        for i, evt in enumerate(FUNNEL_EVENTS):
            entry[FUNNEL_LABELS[i]] = int(row[evt])
        source_data.append(entry)

    # --- Query 7: Daily Trend ---
    print("[8/42] Daily trend...")
    daily_sql = """
        SELECT
            event_date,
            COUNT(DISTINCT uid) AS daily_users,
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN uid END) AS daily_purchases,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN uid END)
                  / NULLIF(COUNT(DISTINCT uid), 0), 1) AS daily_conv_rate
        FROM events
        GROUP BY event_date
        ORDER BY event_date
    """
    daily_raw = query(daily_sql)
    daily_data = []
    for _, row in daily_raw.iterrows():
        daily_data.append({
            "date": str(row["event_date"])[:10],
            "users": int(row["daily_users"]),
            "purchases": int(row["daily_purchases"]),
            "conv_rate": float(row["daily_conv_rate"]) if row["daily_conv_rate"] else 0,
        })

    # --- Query 8: Event Distribution ---
    event_dist_sql = """
        SELECT event_name, COUNT(*) AS event_count, COUNT(DISTINCT uid) AS user_count
        FROM events
        GROUP BY event_name
        ORDER BY event_count DESC
    """
    event_dist_raw = query(event_dist_sql)
    event_dist = []
    for _, row in event_dist_raw.iterrows():
        event_dist.append({
            "event": row["event_name"],
            "events": int(row["event_count"]),
            "users": int(row["user_count"]),
        })

    # --- Query 9: Platform Distribution ---
    print("[9/42] Platform distribution...")
    platform_sql = """
        SELECT
            COALESCE(NULLIF(platform, ''), 'Unknown') AS platform,
            COUNT(DISTINCT uid) AS users,
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN uid END) AS purchasers
        FROM events
        GROUP BY platform
        ORDER BY users DESC
    """
    platform_raw = query(platform_sql)
    platform_data = []
    for _, row in platform_raw.iterrows():
        platform_data.append({
            "platform": row["platform"],
            "users": int(row["users"]),
            "purchasers": int(row["purchasers"]),
        })

    # --- Query 10: Purchase Frequency Distribution ---
    print("[10/42] Purchase frequency distribution...")
    pf_sql = """
        WITH purchase_counts AS (
            SELECT uid, COUNT(*) AS purchase_count
            FROM events
            WHERE event_name = 'purchase'
            GROUP BY uid
        )
        SELECT purchase_count, COUNT(*) AS user_count
        FROM purchase_counts
        GROUP BY purchase_count
        ORDER BY purchase_count
    """
    pf_raw = query(pf_sql)
    purchase_freq = []
    for _, row in pf_raw.iterrows():
        purchase_freq.append({
            "purchases": int(row["purchase_count"]),
            "users": int(row["user_count"]),
        })

    # --- Query 11: KPI per source ---
    print("[11/42] KPI per source...")
    kpi_per_source_sql = """
        WITH uid_source AS (
            SELECT uid, source_group
            FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        ),
        base AS (
            SELECT us.source_group,
                COUNT(*) AS total_users,
                COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.uid END) AS purchasers
            FROM uid_source us
            LEFT JOIN events e ON e.uid = us.uid AND e.event_name = 'purchase'
            GROUP BY us.source_group
        ),
        avg_events AS (
            SELECT us.source_group,
                ROUND(AVG(ue.cnt), 1) AS avg_events_per_user
            FROM uid_source us
            JOIN (SELECT uid, COUNT(*) AS cnt FROM events GROUP BY uid) ue ON ue.uid = us.uid
            GROUP BY us.source_group
        ),
        purchase_seq AS (
            SELECT e.uid, e.event_ts,
                ROW_NUMBER() OVER (PARTITION BY e.uid ORDER BY e.event_ts) AS seq
            FROM events e
            WHERE e.event_name = 'purchase'
        ),
        time_to_first AS (
            SELECT us.source_group,
                ROUND(AVG((unix_timestamp(ps.event_ts) - unix_timestamp(uf.first_touch)) / 3600.0), 1)
                    AS avg_hours_to_first_purchase
            FROM uid_source us
            JOIN purchase_seq ps ON ps.uid = us.uid AND ps.seq = 1
            JOIN (SELECT uid, MIN(event_ts) AS first_touch FROM events GROUP BY uid) uf ON uf.uid = us.uid
            WHERE uf.first_touch < ps.event_ts
            GROUP BY us.source_group
        ),
        time_to_repeat AS (
            SELECT us.source_group,
                ROUND(AVG((unix_timestamp(p2.event_ts) - unix_timestamp(p1.event_ts)) / 3600.0), 1)
                    AS avg_hours_to_repeat
            FROM uid_source us
            JOIN purchase_seq p1 ON p1.uid = us.uid AND p1.seq = 1
            JOIN purchase_seq p2 ON p2.uid = us.uid AND p2.seq = 2
            GROUP BY us.source_group
        ),
        repeat_pct AS (
            SELECT us.source_group,
                ROUND(100.0 * COUNT(CASE WHEN pc.cnt > 1 THEN 1 END)
                    / NULLIF(COUNT(*), 0), 1) AS repeat_purchase_pct
            FROM uid_source us
            JOIN (SELECT uid, COUNT(*) AS cnt FROM events WHERE event_name = 'purchase' GROUP BY uid) pc
                ON pc.uid = us.uid
            GROUP BY us.source_group
        )
        SELECT
            b.source_group, b.total_users, b.purchasers,
            ae.avg_events_per_user,
            tf.avg_hours_to_first_purchase,
            tr.avg_hours_to_repeat,
            rp.repeat_purchase_pct
        FROM base b
        LEFT JOIN avg_events ae ON ae.source_group = b.source_group
        LEFT JOIN time_to_first tf ON tf.source_group = b.source_group
        LEFT JOIN time_to_repeat tr ON tr.source_group = b.source_group
        LEFT JOIN repeat_pct rp ON rp.source_group = b.source_group
    """
    kpi_source_raw = query(kpi_per_source_sql)

    # --- Query 12: Daily trend per source ---
    print("[12/42] Daily trend per source...")
    daily_source_sql = """
        WITH uid_source AS (
            SELECT uid, source_group
            FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        )
        SELECT
            us.source_group,
            e.event_date,
            COUNT(DISTINCT e.uid) AS daily_users,
            COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.uid END) AS daily_purchases,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.uid END)
                  / NULLIF(COUNT(DISTINCT e.uid), 0), 1) AS daily_conv_rate
        FROM events e
        JOIN uid_source us ON e.uid = us.uid
        GROUP BY us.source_group, e.event_date
        ORDER BY us.source_group, e.event_date
    """
    daily_source_raw = query(daily_source_sql)

    # --- Query 13: Purchase frequency per source ---
    print("[13/42] Purchase frequency per source...")
    pf_source_sql = """
        WITH uid_source AS (
            SELECT uid, source_group
            FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        ),
        purchase_counts AS (
            SELECT e.uid, us.source_group, COUNT(*) AS purchase_count
            FROM events e
            JOIN uid_source us ON e.uid = us.uid
            WHERE e.event_name = 'purchase'
            GROUP BY e.uid, us.source_group
        )
        SELECT source_group, purchase_count, COUNT(*) AS user_count
        FROM purchase_counts
        GROUP BY source_group, purchase_count
        ORDER BY source_group, purchase_count
    """
    pf_source_raw = query(pf_source_sql)

    # --- Query 14: Revenue Trend ---
    print("[14/42] Revenue trend...")
    rev_trend_sql = """
        SELECT
            event_date,
            SUM(CASE WHEN event_name = 'purchase' AND transaction_id IS NOT NULL THEN revenue ELSE 0 END) AS daily_revenue,
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' AND transaction_id IS NOT NULL THEN transaction_id END) AS daily_orders,
            ROUND(
                SUM(CASE WHEN event_name = 'purchase' AND transaction_id IS NOT NULL THEN revenue ELSE 0 END) /
                NULLIF(COUNT(DISTINCT CASE WHEN event_name = 'purchase' AND transaction_id IS NOT NULL THEN transaction_id END), 0),
            0) AS aov
        FROM events
        GROUP BY event_date
        ORDER BY event_date
    """
    rev_trend_raw = query(rev_trend_sql)
    revenue_trend = []
    for _, row in rev_trend_raw.iterrows():
        revenue_trend.append({
            "date": str(row["event_date"])[:10],
            "revenue": float(row["daily_revenue"]),
            "orders": int(row["daily_orders"]),
            "aov": float(row["aov"]) if row["aov"] else 0,
        })

    # --- Query 15: Hourly Heatmap ---
    print("[15/42] Hourly heatmap...")
    heatmap_sql = """
        SELECT
            day_of_week, event_hour,
            COUNT(DISTINCT uid) AS users,
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN transaction_id END) AS orders,
            SUM(CASE WHEN event_name = 'purchase' THEN revenue ELSE 0 END) AS revenue
        FROM events
        GROUP BY day_of_week, event_hour
        ORDER BY day_of_week, event_hour
    """
    heatmap_raw = query(heatmap_sql)
    hourly_heatmap = []
    for _, row in heatmap_raw.iterrows():
        hourly_heatmap.append({
            "day": int(row["day_of_week"]),
            "hour": int(row["event_hour"]),
            "users": int(row["users"]),
            "orders": int(row["orders"]),
            "revenue": float(row["revenue"]),
        })

    # --- Query 16: AOV Distribution ---
    print("[16/42] AOV distribution...")
    aov_sql = """
        WITH orders AS (
            SELECT transaction_id, SUM(revenue) AS order_value
            FROM events
            WHERE event_name = 'purchase' AND transaction_id IS NOT NULL
            GROUP BY transaction_id
        )
        SELECT
            CASE
                WHEN order_value < 100 THEN '0-99'
                WHEN order_value < 500 THEN '100-499'
                WHEN order_value < 1000 THEN '500-999'
                WHEN order_value < 2000 THEN '1K-1.9K'
                WHEN order_value < 5000 THEN '2K-4.9K'
                ELSE '5K+'
            END AS bucket,
            COUNT(*) AS order_count,
            ROUND(AVG(order_value), 0) AS avg_value
        FROM orders
        GROUP BY
            CASE
                WHEN order_value < 100 THEN '0-99'
                WHEN order_value < 500 THEN '100-499'
                WHEN order_value < 1000 THEN '500-999'
                WHEN order_value < 2000 THEN '1K-1.9K'
                WHEN order_value < 5000 THEN '2K-4.9K'
                ELSE '5K+'
            END
        ORDER BY MIN(order_value)
    """
    aov_raw = query(aov_sql)
    aov_distribution = []
    for _, row in aov_raw.iterrows():
        aov_distribution.append({
            "bucket": row["bucket"],
            "orders": int(row["order_count"]),
            "avg_value": float(row["avg_value"]) if row["avg_value"] else 0,
        })

    # --- Query 17: New vs Returning ---
    print("[17/42] New vs returning...")
    nvr_sql = """
        WITH user_type AS (
            SELECT uid,
                CASE WHEN MIN(session_number) = 1 THEN 'New' ELSE 'Returning' END AS user_type
            FROM events
            WHERE session_number IS NOT NULL AND session_number > 0
            GROUP BY uid
        )
        SELECT
            ut.user_type,
            COUNT(DISTINCT ut.uid) AS users,
            SUM(CASE WHEN e.event_name = 'purchase' THEN e.revenue ELSE 0 END) AS revenue,
            COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.uid END) AS purchasers
        FROM user_type ut
        JOIN events e ON e.uid = ut.uid
        GROUP BY ut.user_type
    """
    nvr_raw = query(nvr_sql)
    new_vs_returning = []
    for _, row in nvr_raw.iterrows():
        users = int(row["users"])
        purchasers = int(row["purchasers"])
        new_vs_returning.append({
            "type": row["user_type"],
            "users": users,
            "revenue": float(row["revenue"]),
            "purchasers": purchasers,
            "cvr": round(100.0 * purchasers / users, 1) if users else 0,
        })

    # --- Query 18: Revenue by Source (global only) ---
    print("[18/42] Revenue by source...")
    rev_source_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        )
        SELECT
            us.source_group,
            SUM(CASE WHEN e.event_name = 'purchase' THEN e.revenue ELSE 0 END) AS revenue,
            COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.transaction_id END) AS orders
        FROM events e
        JOIN uid_source us ON e.uid = us.uid
        GROUP BY us.source_group
        ORDER BY revenue DESC
    """
    rev_source_raw = query(rev_source_sql)
    revenue_by_source = []
    for _, row in rev_source_raw.iterrows():
        revenue_by_source.append({
            "source": row["source_group"],
            "revenue": float(row["revenue"]),
            "orders": int(row["orders"]),
        })

    # --- Query 19: Session Depth vs CVR ---
    print("[19/42] Session depth CVR...")
    sess_cvr_sql = """
        WITH user_max_session AS (
            SELECT uid, MAX(session_number) AS max_session
            FROM events
            WHERE session_number IS NOT NULL AND session_number > 0
            GROUP BY uid
        ),
        user_bucket AS (
            SELECT uid, max_session,
                CASE
                    WHEN max_session = 1 THEN '1'
                    WHEN max_session = 2 THEN '2'
                    WHEN max_session <= 5 THEN '3-5'
                    WHEN max_session <= 10 THEN '6-10'
                    ELSE '11+'
                END AS session_bucket
            FROM user_max_session
        )
        SELECT
            ub.session_bucket,
            COUNT(DISTINCT ub.uid) AS users,
            COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.uid END) AS purchasers,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.uid END)
                / NULLIF(COUNT(DISTINCT ub.uid), 0), 1) AS cvr
        FROM user_bucket ub
        JOIN events e ON e.uid = ub.uid
        GROUP BY ub.session_bucket
        ORDER BY MIN(ub.max_session)
    """
    sess_cvr_raw = query(sess_cvr_sql)
    session_depth_cvr = []
    for _, row in sess_cvr_raw.iterrows():
        session_depth_cvr.append({
            "bucket": row["session_bucket"],
            "users": int(row["users"]),
            "purchasers": int(row["purchasers"]),
            "cvr": float(row["cvr"]) if row["cvr"] else 0,
        })

    # --- Query 20: Engagement Time Distribution ---
    print("[20/42] Engagement distribution...")
    engage_sql = """
        WITH user_engagement AS (
            SELECT uid,
                SUM(engagement_msec) / 1000.0 AS total_sec,
                MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS is_purchaser
            FROM events
            GROUP BY uid
        )
        SELECT
            CASE
                WHEN total_sec < 10 THEN '0-10s'
                WHEN total_sec < 30 THEN '10-30s'
                WHEN total_sec < 60 THEN '30-60s'
                WHEN total_sec < 180 THEN '1-3m'
                WHEN total_sec < 600 THEN '3-10m'
                ELSE '10m+'
            END AS bucket,
            is_purchaser,
            COUNT(*) AS user_count
        FROM user_engagement
        GROUP BY
            CASE
                WHEN total_sec < 10 THEN '0-10s'
                WHEN total_sec < 30 THEN '10-30s'
                WHEN total_sec < 60 THEN '30-60s'
                WHEN total_sec < 180 THEN '1-3m'
                WHEN total_sec < 600 THEN '3-10m'
                ELSE '10m+'
            END,
            is_purchaser
        ORDER BY
            CASE
                WHEN bucket = '0-10s' THEN 1 WHEN bucket = '10-30s' THEN 2 WHEN bucket = '30-60s' THEN 3
                WHEN bucket = '1-3m' THEN 4 WHEN bucket = '3-10m' THEN 5 ELSE 6
            END,
            is_purchaser
    """
    engage_raw = query(engage_sql)
    engagement_distribution = []
    buckets_seen = {}
    for _, row in engage_raw.iterrows():
        b = row["bucket"]
        is_p = int(row["is_purchaser"])
        uc = int(row["user_count"])
        if b not in buckets_seen:
            buckets_seen[b] = {"bucket": b, "purchasers": 0, "non_purchasers": 0}
        if is_p:
            buckets_seen[b]["purchasers"] = uc
        else:
            buckets_seen[b]["non_purchasers"] = uc
    bucket_order = ['0-10s', '10-30s', '30-60s', '1-3m', '3-10m', '10m+']
    for b in bucket_order:
        if b in buckets_seen:
            engagement_distribution.append(buckets_seen[b])

    # --- Query 21: Hourly Revenue ---
    print("[21/42] Hourly revenue...")
    hourly_rev_sql = """
        SELECT event_hour,
            SUM(CASE WHEN event_name = 'purchase' THEN revenue ELSE 0 END) AS revenue,
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN transaction_id END) AS orders
        FROM events
        GROUP BY event_hour
        ORDER BY event_hour
    """
    hourly_rev_raw = query(hourly_rev_sql)
    hourly_revenue = []
    for _, row in hourly_rev_raw.iterrows():
        hourly_revenue.append({
            "hour": int(row["event_hour"]),
            "revenue": float(row["revenue"]),
            "orders": int(row["orders"]),
        })

    # --- Query 22: Day of Week Performance ---
    print("[22/42] Day of week...")
    dow_sql = """
        SELECT day_of_week,
            COUNT(DISTINCT uid) AS users,
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN transaction_id END) AS orders,
            SUM(CASE WHEN event_name = 'purchase' THEN revenue ELSE 0 END) AS revenue,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN uid END)
                / NULLIF(COUNT(DISTINCT uid), 0), 1) AS cvr
        FROM events
        GROUP BY day_of_week
        ORDER BY day_of_week
    """
    dow_raw = query(dow_sql)
    day_names = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
    day_of_week_data = []
    for _, row in dow_raw.iterrows():
        d = int(row["day_of_week"])
        day_of_week_data.append({
            "day": d,
            "day_name": day_names[d - 1] if 1 <= d <= 7 else str(d),
            "users": int(row["users"]),
            "orders": int(row["orders"]),
            "revenue": float(row["revenue"]),
            "cvr": float(row["cvr"]) if row["cvr"] else 0,
        })

    # --- Query 23: Top Items ---
    print("[23/42] Top items...")
    items_sql = """
        SELECT first_item_name AS item_name,
            SUM(CASE WHEN event_name = 'purchase' THEN revenue ELSE 0 END) AS revenue,
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN transaction_id END) AS orders,
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN uid END) AS buyers
        FROM events
        WHERE first_item_name IS NOT NULL AND first_item_name != ''
        GROUP BY first_item_name
        ORDER BY revenue DESC
        LIMIT 15
    """
    items_raw = query(items_sql)
    top_items = []
    for _, row in items_raw.iterrows():
        top_items.append({
            "item": row["item_name"],
            "revenue": float(row["revenue"]),
            "orders": int(row["orders"]),
            "buyers": int(row["buyers"]),
        })

    # --- Query 34: Device & Browser CVR ---
    print("[34/42] Device & browser CVR...")
    device_browser_sql = """
        WITH purchasers AS (
            SELECT DISTINCT uid FROM events WHERE event_name = 'purchase'
        )
        SELECT
            COALESCE(device_category, 'Unknown') AS device_category,
            COALESCE(os, 'Unknown') AS os,
            COALESCE(browser_final, 'Unknown') AS browser,
            COUNT(DISTINCT e.uid) AS users,
            COUNT(DISTINCT p.uid) AS purchasers,
            ROUND(100.0 * COUNT(DISTINCT p.uid) / NULLIF(COUNT(DISTINCT e.uid), 0), 1) AS cvr
        FROM events e
        LEFT JOIN purchasers p ON e.uid = p.uid
        GROUP BY device_category, os, browser_final
        ORDER BY users DESC
    """
    device_browser_raw = query(device_browser_sql)
    device_browser_data = []
    for _, row in device_browser_raw.iterrows():
        device_browser_data.append({
            "device": row["device_category"],
            "os": row["os"],
            "browser": row["browser"],
            "users": int(row["users"]),
            "purchasers": int(row["purchasers"]),
            "cvr": float(row["cvr"]) if row["cvr"] else 0,
        })

    # --- Query 35: Geo Region ---
    print("[35/42] Geo region...")
    geo_region_sql = """
        WITH purchasers AS (
            SELECT DISTINCT uid FROM events WHERE event_name = 'purchase'
        )
        SELECT
            COALESCE(geo_region, 'Unknown') AS region,
            COUNT(DISTINCT e.uid) AS users,
            COUNT(DISTINCT p.uid) AS purchasers,
            SUM(CASE WHEN e.event_name = 'purchase' THEN e.revenue ELSE 0 END) AS revenue,
            ROUND(100.0 * COUNT(DISTINCT p.uid) / NULLIF(COUNT(DISTINCT e.uid), 0), 1) AS cvr
        FROM events e
        LEFT JOIN purchasers p ON e.uid = p.uid
        GROUP BY geo_region
        ORDER BY users DESC
        LIMIT 15
    """
    geo_region_raw = query(geo_region_sql)
    geo_region_data = []
    for _, row in geo_region_raw.iterrows():
        geo_region_data.append({
            "region": row["region"],
            "users": int(row["users"]),
            "purchasers": int(row["purchasers"]),
            "revenue": float(row["revenue"]),
            "cvr": float(row["cvr"]) if row["cvr"] else 0,
        })

    # --- Query 36: Campaign Performance (from raw table) ---
    print("[36/42] Campaign performance...")
    campaign_sql = f"""
        WITH raw_campaigns AS (
            SELECT
                user_id_final AS uid,
                event_name,
                COALESCE(ec_purchase_revenue, 0) AS revenue,
                collected_traffic_source.manual_source AS manual_source,
                collected_traffic_source.manual_medium AS manual_medium,
                collected_traffic_source.manual_campaign_name AS manual_campaign_name
            FROM {DATABRICKS_TABLE}
            WHERE user_id_final LIKE 'U%'
              AND p_event_date >= '{DATE_FROM}'
              AND collected_traffic_source.manual_campaign_name IS NOT NULL
              AND collected_traffic_source.manual_campaign_name != 'None'
        ),
        purchasers AS (
            SELECT DISTINCT uid FROM raw_campaigns WHERE event_name = 'purchase'
        )
        SELECT
            COALESCE(rc.manual_source, 'unknown') AS source,
            COALESCE(rc.manual_medium, 'unknown') AS medium,
            COALESCE(rc.manual_campaign_name, 'unknown') AS campaign,
            COUNT(DISTINCT rc.uid) AS users,
            COUNT(DISTINCT p.uid) AS purchasers,
            SUM(CASE WHEN rc.event_name = 'purchase' THEN rc.revenue ELSE 0 END) AS revenue,
            ROUND(100.0 * COUNT(DISTINCT p.uid) / NULLIF(COUNT(DISTINCT rc.uid), 0), 1) AS cvr
        FROM raw_campaigns rc
        LEFT JOIN purchasers p ON rc.uid = p.uid
        GROUP BY rc.manual_source, rc.manual_medium, rc.manual_campaign_name
        ORDER BY users DESC
        LIMIT 15
    """
    campaign_raw = query(campaign_sql)
    campaign_data = []
    for _, row in campaign_raw.iterrows():
        campaign_data.append({
            "source": row["source"],
            "medium": row["medium"],
            "campaign": row["campaign"],
            "users": int(row["users"]),
            "purchasers": int(row["purchasers"]),
            "revenue": float(row["revenue"]),
            "cvr": float(row["cvr"]) if row["cvr"] else 0,
        })

    # --- Query 37: Page Path CVR ---
    print("[37/42] Page path CVR...")
    page_path_sql = """
        WITH page_visits AS (
            SELECT uid,
                CASE
                    WHEN page_location LIKE '%/cart%' THEN '/cart'
                    WHEN page_location LIKE '%/lottoboard/series%' THEN '/lottoboard/series'
                    WHEN page_location LIKE '%/lottoboard/all%' THEN '/lottoboard/all'
                    WHEN page_location LIKE '%/lottoboard/single%' THEN '/lottoboard/single'
                    WHEN page_location LIKE '%/safe%' THEN '/safe'
                    WHEN page_location LIKE '%/highlights%' THEN '/highlights'
                    WHEN page_location LIKE '%/jidrid%' THEN '/jidrid'
                    WHEN page_location LIKE '%/profile%' THEN '/profile'
                    WHEN page_location LIKE '%/orders%' THEN '/orders'
                    WHEN page_location LIKE '%/register%' THEN '/register'
                    WHEN page_location LIKE '%/dashboard%' THEN '/dashboard'
                    WHEN page_location LIKE '%/prize-history%' THEN '/prize-history'
                    ELSE '/ (home)'
                END AS page_path
            FROM events
            WHERE page_location IS NOT NULL
        ),
        purchasers AS (
            SELECT DISTINCT uid FROM events WHERE event_name = 'purchase'
        )
        SELECT
            pv.page_path,
            COUNT(DISTINCT pv.uid) AS visitors,
            COUNT(DISTINCT p.uid) AS purchasers,
            ROUND(100.0 * COUNT(DISTINCT p.uid) / NULLIF(COUNT(DISTINCT pv.uid), 0), 1) AS cvr
        FROM page_visits pv
        LEFT JOIN purchasers p ON pv.uid = p.uid
        GROUP BY pv.page_path
        ORDER BY cvr DESC
    """
    page_path_raw = query(page_path_sql)
    page_path_data = []
    for _, row in page_path_raw.iterrows():
        page_path_data.append({
            "path": row["page_path"],
            "visitors": int(row["visitors"]),
            "purchasers": int(row["purchasers"]),
            "cvr": float(row["cvr"]) if row["cvr"] else 0,
        })

    # --- Query 38: Event Distribution Full (all events) ---
    print("[38/42] Event distribution full...")
    event_dist_full_sql = """
        SELECT event_name,
            COUNT(*) AS event_count,
            COUNT(DISTINCT uid) AS user_count
        FROM events
        GROUP BY event_name
        ORDER BY event_count DESC
    """
    event_dist_full_raw = query(event_dist_full_sql)
    event_dist_full_data = []
    for _, row in event_dist_full_raw.iterrows():
        event_dist_full_data.append({
            "event": row["event_name"],
            "events": int(row["event_count"]),
            "users": int(row["user_count"]),
        })

    # --- Per-Source: Revenue Trend ---
    print("[24/42] Revenue trend per source...")
    rev_trend_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        )
        SELECT us.source_group, e.event_date,
            SUM(CASE WHEN e.event_name = 'purchase' AND e.transaction_id IS NOT NULL THEN e.revenue ELSE 0 END) AS daily_revenue,
            COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' AND e.transaction_id IS NOT NULL THEN e.transaction_id END) AS daily_orders,
            ROUND(
                SUM(CASE WHEN e.event_name = 'purchase' AND e.transaction_id IS NOT NULL THEN e.revenue ELSE 0 END) /
                NULLIF(COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' AND e.transaction_id IS NOT NULL THEN e.transaction_id END), 0),
            0) AS aov
        FROM events e
        JOIN uid_source us ON e.uid = us.uid
        GROUP BY us.source_group, e.event_date
        ORDER BY us.source_group, e.event_date
    """
    rev_trend_src_raw = query(rev_trend_src_sql)

    # --- Per-Source: Hourly Heatmap ---
    print("[25/42] Hourly heatmap per source...")
    heatmap_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        )
        SELECT us.source_group, e.day_of_week, e.event_hour,
            COUNT(DISTINCT e.uid) AS users,
            COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.transaction_id END) AS orders,
            SUM(CASE WHEN e.event_name = 'purchase' THEN e.revenue ELSE 0 END) AS revenue
        FROM events e
        JOIN uid_source us ON e.uid = us.uid
        GROUP BY us.source_group, e.day_of_week, e.event_hour
        ORDER BY us.source_group, e.day_of_week, e.event_hour
    """
    heatmap_src_raw = query(heatmap_src_sql)

    # --- Per-Source: AOV Distribution ---
    print("[26/42] AOV distribution per source...")
    aov_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        ),
        orders AS (
            SELECT e.transaction_id, us.source_group, SUM(e.revenue) AS order_value
            FROM events e
            JOIN uid_source us ON e.uid = us.uid
            WHERE e.event_name = 'purchase' AND e.transaction_id IS NOT NULL
            GROUP BY e.transaction_id, us.source_group
        )
        SELECT source_group,
            CASE
                WHEN order_value < 100 THEN '0-99'
                WHEN order_value < 500 THEN '100-499'
                WHEN order_value < 1000 THEN '500-999'
                WHEN order_value < 2000 THEN '1K-1.9K'
                WHEN order_value < 5000 THEN '2K-4.9K'
                ELSE '5K+'
            END AS bucket,
            COUNT(*) AS order_count,
            ROUND(AVG(order_value), 0) AS avg_value
        FROM orders
        GROUP BY source_group,
            CASE
                WHEN order_value < 100 THEN '0-99'
                WHEN order_value < 500 THEN '100-499'
                WHEN order_value < 1000 THEN '500-999'
                WHEN order_value < 2000 THEN '1K-1.9K'
                WHEN order_value < 5000 THEN '2K-4.9K'
                ELSE '5K+'
            END
        ORDER BY source_group, MIN(order_value)
    """
    aov_src_raw = query(aov_src_sql)

    # --- Per-Source: New vs Returning ---
    print("[27/42] New vs returning per source...")
    nvr_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        ),
        user_type AS (
            SELECT uid,
                CASE WHEN MIN(session_number) = 1 THEN 'New' ELSE 'Returning' END AS user_type
            FROM events
            WHERE session_number IS NOT NULL AND session_number > 0
            GROUP BY uid
        )
        SELECT us.source_group, ut.user_type,
            COUNT(DISTINCT ut.uid) AS users,
            SUM(CASE WHEN e.event_name = 'purchase' THEN e.revenue ELSE 0 END) AS revenue,
            COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.uid END) AS purchasers
        FROM user_type ut
        JOIN uid_source us ON ut.uid = us.uid
        JOIN events e ON e.uid = ut.uid
        GROUP BY us.source_group, ut.user_type
    """
    nvr_src_raw = query(nvr_src_sql)

    # --- Per-Source: Session Depth CVR ---
    print("[28/42] Session depth CVR per source...")
    sess_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        ),
        user_max_session AS (
            SELECT uid, MAX(session_number) AS max_session
            FROM events
            WHERE session_number IS NOT NULL AND session_number > 0
            GROUP BY uid
        ),
        user_bucket AS (
            SELECT uid, max_session,
                CASE
                    WHEN max_session = 1 THEN '1'
                    WHEN max_session = 2 THEN '2'
                    WHEN max_session <= 5 THEN '3-5'
                    WHEN max_session <= 10 THEN '6-10'
                    ELSE '11+'
                END AS session_bucket
            FROM user_max_session
        )
        SELECT us.source_group, ub.session_bucket,
            COUNT(DISTINCT ub.uid) AS users,
            COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.uid END) AS purchasers,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.uid END)
                / NULLIF(COUNT(DISTINCT ub.uid), 0), 1) AS cvr
        FROM user_bucket ub
        JOIN uid_source us ON ub.uid = us.uid
        JOIN events e ON e.uid = ub.uid
        GROUP BY us.source_group, ub.session_bucket
        ORDER BY us.source_group, MIN(ub.max_session)
    """
    sess_src_raw = query(sess_src_sql)

    # --- Per-Source: Engagement Distribution ---
    print("[29/42] Engagement per source...")
    engage_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        ),
        user_engagement AS (
            SELECT uid,
                SUM(engagement_msec) / 1000.0 AS total_sec,
                MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS is_purchaser
            FROM events
            GROUP BY uid
        )
        SELECT us.source_group,
            CASE
                WHEN ue.total_sec < 10 THEN '0-10s'
                WHEN ue.total_sec < 30 THEN '10-30s'
                WHEN ue.total_sec < 60 THEN '30-60s'
                WHEN ue.total_sec < 180 THEN '1-3m'
                WHEN ue.total_sec < 600 THEN '3-10m'
                ELSE '10m+'
            END AS bucket,
            ue.is_purchaser,
            COUNT(*) AS user_count
        FROM user_engagement ue
        JOIN uid_source us ON ue.uid = us.uid
        GROUP BY us.source_group,
            CASE
                WHEN ue.total_sec < 10 THEN '0-10s'
                WHEN ue.total_sec < 30 THEN '10-30s'
                WHEN ue.total_sec < 60 THEN '30-60s'
                WHEN ue.total_sec < 180 THEN '1-3m'
                WHEN ue.total_sec < 600 THEN '3-10m'
                ELSE '10m+'
            END,
            ue.is_purchaser
    """
    engage_src_raw = query(engage_src_sql)

    # --- Per-Source: Hourly Revenue ---
    print("[30/42] Hourly revenue per source...")
    hourly_rev_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        )
        SELECT us.source_group, e.event_hour,
            SUM(CASE WHEN e.event_name = 'purchase' THEN e.revenue ELSE 0 END) AS revenue,
            COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.transaction_id END) AS orders
        FROM events e
        JOIN uid_source us ON e.uid = us.uid
        GROUP BY us.source_group, e.event_hour
        ORDER BY us.source_group, e.event_hour
    """
    hourly_rev_src_raw = query(hourly_rev_src_sql)

    # --- Per-Source: Day of Week ---
    print("[31/42] Day of week per source...")
    dow_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        )
        SELECT us.source_group, e.day_of_week,
            COUNT(DISTINCT e.uid) AS users,
            COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.transaction_id END) AS orders,
            SUM(CASE WHEN e.event_name = 'purchase' THEN e.revenue ELSE 0 END) AS revenue,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.uid END)
                / NULLIF(COUNT(DISTINCT e.uid), 0), 1) AS cvr
        FROM events e
        JOIN uid_source us ON e.uid = us.uid
        GROUP BY us.source_group, e.day_of_week
        ORDER BY us.source_group, e.day_of_week
    """
    dow_src_raw = query(dow_src_sql)

    # --- Per-Source: Top Items ---
    print("[32/42] Top items per source...")
    items_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        ),
        item_stats AS (
            SELECT us.source_group, e.first_item_name AS item_name,
                SUM(CASE WHEN e.event_name = 'purchase' THEN e.revenue ELSE 0 END) AS revenue,
                COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.transaction_id END) AS orders,
                COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.uid END) AS buyers,
                ROW_NUMBER() OVER (
                    PARTITION BY us.source_group
                    ORDER BY SUM(CASE WHEN e.event_name = 'purchase' THEN e.revenue ELSE 0 END) DESC
                ) AS rn
            FROM events e
            JOIN uid_source us ON e.uid = us.uid
            WHERE e.first_item_name IS NOT NULL AND e.first_item_name != ''
            GROUP BY us.source_group, e.first_item_name
        )
        SELECT source_group, item_name, revenue, orders, buyers
        FROM item_stats WHERE rn <= 15
        ORDER BY source_group, revenue DESC
    """
    items_src_raw = query(items_src_sql)

    # --- Per-Source: Device & Browser CVR ---
    print("[39/42] Device & browser per source...")
    device_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        ),
        purchasers AS (
            SELECT DISTINCT uid FROM events WHERE event_name = 'purchase'
        )
        SELECT
            us.source_group,
            COALESCE(e.device_category, 'Unknown') AS device_category,
            COALESCE(e.os, 'Unknown') AS os,
            COALESCE(e.browser_final, 'Unknown') AS browser,
            COUNT(DISTINCT e.uid) AS users,
            COUNT(DISTINCT p.uid) AS purchasers,
            ROUND(100.0 * COUNT(DISTINCT p.uid) / NULLIF(COUNT(DISTINCT e.uid), 0), 1) AS cvr
        FROM events e
        JOIN uid_source us ON e.uid = us.uid
        LEFT JOIN purchasers p ON e.uid = p.uid
        GROUP BY us.source_group, e.device_category, e.os, e.browser_final
        ORDER BY us.source_group, users DESC
    """
    device_src_raw = query(device_src_sql)

    # --- Per-Source: Geo Region ---
    print("[40/42] Geo region per source...")
    geo_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        ),
        purchasers AS (
            SELECT DISTINCT uid FROM events WHERE event_name = 'purchase'
        )
        SELECT
            us.source_group,
            COALESCE(e.geo_region, 'Unknown') AS region,
            COUNT(DISTINCT e.uid) AS users,
            COUNT(DISTINCT p.uid) AS purchasers,
            SUM(CASE WHEN e.event_name = 'purchase' THEN e.revenue ELSE 0 END) AS revenue,
            ROUND(100.0 * COUNT(DISTINCT p.uid) / NULLIF(COUNT(DISTINCT e.uid), 0), 1) AS cvr
        FROM events e
        JOIN uid_source us ON e.uid = us.uid
        LEFT JOIN purchasers p ON e.uid = p.uid
        GROUP BY us.source_group, e.geo_region
        ORDER BY us.source_group, users DESC
    """
    geo_src_raw = query(geo_src_sql)

    # --- Per-Source: Page Path CVR ---
    print("[41/42] Page path CVR per source...")
    page_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        ),
        page_visits AS (
            SELECT uid,
                CASE
                    WHEN page_location LIKE '%/cart%' THEN '/cart'
                    WHEN page_location LIKE '%/lottoboard/series%' THEN '/lottoboard/series'
                    WHEN page_location LIKE '%/lottoboard/all%' THEN '/lottoboard/all'
                    WHEN page_location LIKE '%/lottoboard/single%' THEN '/lottoboard/single'
                    WHEN page_location LIKE '%/safe%' THEN '/safe'
                    WHEN page_location LIKE '%/highlights%' THEN '/highlights'
                    WHEN page_location LIKE '%/jidrid%' THEN '/jidrid'
                    WHEN page_location LIKE '%/profile%' THEN '/profile'
                    WHEN page_location LIKE '%/orders%' THEN '/orders'
                    WHEN page_location LIKE '%/register%' THEN '/register'
                    WHEN page_location LIKE '%/dashboard%' THEN '/dashboard'
                    WHEN page_location LIKE '%/prize-history%' THEN '/prize-history'
                    ELSE '/ (home)'
                END AS page_path
            FROM events
            WHERE page_location IS NOT NULL
        ),
        purchasers AS (
            SELECT DISTINCT uid FROM events WHERE event_name = 'purchase'
        )
        SELECT
            us.source_group,
            pv.page_path,
            COUNT(DISTINCT pv.uid) AS visitors,
            COUNT(DISTINCT p.uid) AS purchasers,
            ROUND(100.0 * COUNT(DISTINCT p.uid) / NULLIF(COUNT(DISTINCT pv.uid), 0), 1) AS cvr
        FROM page_visits pv
        JOIN uid_source us ON pv.uid = us.uid
        LEFT JOIN purchasers p ON pv.uid = p.uid
        GROUP BY us.source_group, pv.page_path
        ORDER BY us.source_group, cvr DESC
    """
    page_src_raw = query(page_src_sql)

    # --- Per-Source: Event Distribution Full ---
    print("[42/42] Event distribution per source...")
    event_src_sql = """
        WITH uid_source AS (
            SELECT uid, source_group FROM (
                SELECT uid, source_group,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts) AS rn
                FROM events
            ) t WHERE rn = 1
        )
        SELECT
            us.source_group,
            e.event_name,
            COUNT(*) AS event_count,
            COUNT(DISTINCT e.uid) AS user_count
        FROM events e
        JOIN uid_source us ON e.uid = us.uid
        GROUP BY us.source_group, e.event_name
        ORDER BY us.source_group, event_count DESC
    """
    event_src_raw = query(event_src_sql)

    # --- Build by_source structure ---
    print("Building output...")
    by_source = {}
    source_groups = set()

    for _, row in kpi_source_raw.iterrows():
        source_groups.add(row["source_group"])

    for sg in sorted(source_groups):
        # KPI per source
        sg_kpi_row = kpi_source_raw[kpi_source_raw["source_group"] == sg].to_dict(orient="records")
        if sg_kpi_row:
            r = sg_kpi_row[0]
            sg_total = int(r["total_users"])
            sg_purchasers = int(r["purchasers"])
            sg_kpi = {
                "total_users": sg_total,
                "conversion_rate": round(100.0 * sg_purchasers / sg_total, 1) if sg_total else 0,
                "purchases": sg_purchasers,
                "avg_events_per_user": float(r["avg_events_per_user"]) if r["avg_events_per_user"] else 0,
                "avg_hours_to_first_purchase": float(r["avg_hours_to_first_purchase"]) if r["avg_hours_to_first_purchase"] else 0,
                "avg_hours_to_repeat": float(r["avg_hours_to_repeat"]) if r["avg_hours_to_repeat"] else 0,
                "repeat_purchase_pct": float(r["repeat_purchase_pct"]) if r["repeat_purchase_pct"] else 0,
            }
        else:
            sg_kpi = {}

        # Funnel per source
        sg_funnel_entry = [s for s in source_data if s["source"] == sg]
        sg_funnel = []
        if sg_funnel_entry:
            entry = sg_funnel_entry[0]
            sg_total_users = entry["total_users"]
            sg_funnel.append({
                "step": "Total Users", "event": "all",
                "users": sg_total_users, "pct_of_total": 100.0, "drop_off_pct": 0
            })
            for i, evt in enumerate(FUNNEL_EVENTS):
                count = entry.get(FUNNEL_LABELS[i], 0)
                prev_count = sg_funnel[-1]["users"]
                sg_funnel.append({
                    "step": FUNNEL_LABELS[i], "event": evt,
                    "users": count,
                    "pct_of_total": round(100.0 * count / sg_total_users, 1) if sg_total_users else 0,
                    "drop_off_pct": round(100.0 * (1 - count / prev_count), 1) if prev_count else 0,
                })

        # Daily trend per source
        sg_daily_rows = daily_source_raw[daily_source_raw["source_group"] == sg]
        sg_daily = []
        for _, row in sg_daily_rows.iterrows():
            sg_daily.append({
                "date": str(row["event_date"])[:10],
                "users": int(row["daily_users"]),
                "purchases": int(row["daily_purchases"]),
                "conv_rate": float(row["daily_conv_rate"]) if row["daily_conv_rate"] else 0,
            })

        # Purchase frequency per source
        sg_pf_rows = pf_source_raw[pf_source_raw["source_group"] == sg]
        sg_pf = []
        for _, row in sg_pf_rows.iterrows():
            sg_pf.append({
                "purchases": int(row["purchase_count"]),
                "users": int(row["user_count"]),
            })

        # Revenue trend per source
        sg_rev_trend_rows = rev_trend_src_raw[rev_trend_src_raw["source_group"] == sg]
        sg_rev_trend = []
        for _, row in sg_rev_trend_rows.iterrows():
            sg_rev_trend.append({
                "date": str(row["event_date"])[:10],
                "revenue": float(row["daily_revenue"]),
                "orders": int(row["daily_orders"]),
                "aov": float(row["aov"]) if row["aov"] else 0,
            })

        # Hourly heatmap per source
        sg_heatmap_rows = heatmap_src_raw[heatmap_src_raw["source_group"] == sg]
        sg_heatmap = []
        for _, row in sg_heatmap_rows.iterrows():
            sg_heatmap.append({
                "day": int(row["day_of_week"]),
                "hour": int(row["event_hour"]),
                "users": int(row["users"]),
                "orders": int(row["orders"]),
                "revenue": float(row["revenue"]),
            })

        # AOV distribution per source
        sg_aov_rows = aov_src_raw[aov_src_raw["source_group"] == sg]
        sg_aov = []
        for _, row in sg_aov_rows.iterrows():
            sg_aov.append({
                "bucket": row["bucket"],
                "orders": int(row["order_count"]),
                "avg_value": float(row["avg_value"]) if row["avg_value"] else 0,
            })

        # New vs returning per source
        sg_nvr_rows = nvr_src_raw[nvr_src_raw["source_group"] == sg]
        sg_nvr = []
        for _, row in sg_nvr_rows.iterrows():
            users = int(row["users"])
            purchasers = int(row["purchasers"])
            sg_nvr.append({
                "type": row["user_type"],
                "users": users,
                "revenue": float(row["revenue"]),
                "purchasers": purchasers,
                "cvr": round(100.0 * purchasers / users, 1) if users else 0,
            })

        # Session depth CVR per source
        sg_sess_rows = sess_src_raw[sess_src_raw["source_group"] == sg]
        sg_sess = []
        for _, row in sg_sess_rows.iterrows():
            sg_sess.append({
                "bucket": row["session_bucket"],
                "users": int(row["users"]),
                "purchasers": int(row["purchasers"]),
                "cvr": float(row["cvr"]) if row["cvr"] else 0,
            })

        # Engagement distribution per source
        sg_engage_rows = engage_src_raw[engage_src_raw["source_group"] == sg]
        sg_engage_buckets = {}
        for _, row in sg_engage_rows.iterrows():
            b = row["bucket"]
            is_p = int(row["is_purchaser"])
            uc = int(row["user_count"])
            if b not in sg_engage_buckets:
                sg_engage_buckets[b] = {"bucket": b, "purchasers": 0, "non_purchasers": 0}
            if is_p:
                sg_engage_buckets[b]["purchasers"] = uc
            else:
                sg_engage_buckets[b]["non_purchasers"] = uc
        sg_engage = [sg_engage_buckets[b] for b in bucket_order if b in sg_engage_buckets]

        # Hourly revenue per source
        sg_hourly_rev_rows = hourly_rev_src_raw[hourly_rev_src_raw["source_group"] == sg]
        sg_hourly_rev = []
        for _, row in sg_hourly_rev_rows.iterrows():
            sg_hourly_rev.append({
                "hour": int(row["event_hour"]),
                "revenue": float(row["revenue"]),
                "orders": int(row["orders"]),
            })

        # Day of week per source
        sg_dow_rows = dow_src_raw[dow_src_raw["source_group"] == sg]
        sg_dow = []
        for _, row in sg_dow_rows.iterrows():
            d = int(row["day_of_week"])
            sg_dow.append({
                "day": d,
                "day_name": day_names[d - 1] if 1 <= d <= 7 else str(d),
                "users": int(row["users"]),
                "orders": int(row["orders"]),
                "revenue": float(row["revenue"]),
                "cvr": float(row["cvr"]) if row["cvr"] else 0,
            })

        # Top items per source
        sg_items_rows = items_src_raw[items_src_raw["source_group"] == sg]
        sg_items = []
        for _, row in sg_items_rows.iterrows():
            sg_items.append({
                "item": row["item_name"],
                "revenue": float(row["revenue"]),
                "orders": int(row["orders"]),
                "buyers": int(row["buyers"]),
            })

        # Device & browser per source
        sg_device_rows = device_src_raw[device_src_raw["source_group"] == sg]
        sg_device = []
        for _, row in sg_device_rows.iterrows():
            sg_device.append({
                "device": row["device_category"],
                "os": row["os"],
                "browser": row["browser"],
                "users": int(row["users"]),
                "purchasers": int(row["purchasers"]),
                "cvr": float(row["cvr"]) if row["cvr"] else 0,
            })

        # Geo region per source (top 15)
        sg_geo_rows = geo_src_raw[geo_src_raw["source_group"] == sg].head(15)
        sg_geo = []
        for _, row in sg_geo_rows.iterrows():
            sg_geo.append({
                "region": row["region"],
                "users": int(row["users"]),
                "purchasers": int(row["purchasers"]),
                "revenue": float(row["revenue"]),
                "cvr": float(row["cvr"]) if row["cvr"] else 0,
            })

        # Page path CVR per source
        sg_page_rows = page_src_raw[page_src_raw["source_group"] == sg]
        sg_page = []
        for _, row in sg_page_rows.iterrows():
            sg_page.append({
                "path": row["page_path"],
                "visitors": int(row["visitors"]),
                "purchasers": int(row["purchasers"]),
                "cvr": float(row["cvr"]) if row["cvr"] else 0,
            })

        # Event distribution per source
        sg_event_rows = event_src_raw[event_src_raw["source_group"] == sg]
        sg_event = []
        for _, row in sg_event_rows.iterrows():
            sg_event.append({
                "event": row["event_name"],
                "events": int(row["event_count"]),
                "users": int(row["user_count"]),
            })

        by_source[sg] = {
            "kpi": sg_kpi,
            "funnel": sg_funnel,
            "daily_trend": sg_daily,
            "purchase_frequency": sg_pf,
            "funnel_by_source": [s for s in source_data if s["source"] == sg],
            "revenue_trend": sg_rev_trend,
            "hourly_heatmap": sg_heatmap,
            "aov_distribution": sg_aov,
            "new_vs_returning": sg_nvr,
            "session_depth_cvr": sg_sess,
            "engagement_distribution": sg_engage,
            "hourly_revenue": sg_hourly_rev,
            "day_of_week": sg_dow,
            "top_items": sg_items,
            "device_browser": sg_device,
            "geo_region": sg_geo,
            "page_path_cvr": sg_page,
            "event_distribution_full": sg_event,
        }

    # --- Assemble & Write JSON ---
    output = {
        "meta": meta,
        "kpi": kpi_data,
        "funnel": funnel_data,
        "sankey": {"nodes": sankey_nodes, "links": sankey_links},
        "journeys": journeys_data,
        "funnel_by_source": source_data,
        "daily_trend": daily_data,
        "event_distribution": event_dist,
        "purchase_frequency": purchase_freq,
        "platform_distribution": platform_data,
        "revenue_trend": revenue_trend,
        "hourly_heatmap": hourly_heatmap,
        "aov_distribution": aov_distribution,
        "new_vs_returning": new_vs_returning,
        "revenue_by_source": revenue_by_source,
        "session_depth_cvr": session_depth_cvr,
        "engagement_distribution": engagement_distribution,
        "hourly_revenue": hourly_revenue,
        "day_of_week": day_of_week_data,
        "top_items": top_items,
        "device_browser": device_browser_data,
        "geo_region": geo_region_data,
        "campaign_performance": campaign_data,
        "page_path_cvr": page_path_data,
        "event_distribution_full": event_dist_full_data,
        "by_source": by_source,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)

    elapsed = time.time() - start
    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"\nDone in {elapsed:.1f}s → {OUTPUT_PATH} ({size_kb:.0f} KB)")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
