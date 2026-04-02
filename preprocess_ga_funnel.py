"""
GA4 Funnel Analysis Pre-processor
Reads 6GB CSV with DuckDB, outputs summarized JSON for the dashboard.
"""
import duckdb
import json
import os
import time

CSV_PATH = "data/ga_traffic_events/ga_traffic_events_20260320_to_now.csv"
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

# Source grouping rules
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
    con = duckdb.connect()

    # Create a view over the CSV for reuse
    print("[1/8] Creating CSV view...")
    con.execute(f"""
        CREATE VIEW events AS
        SELECT
            p_event_date::DATE AS event_date,
            event_ts_th::TIMESTAMP AS event_ts,
            user_id_final::VARCHAR AS uid,
            event_name,
            source,
            platform
        FROM read_csv_auto('{CSV_PATH}', header=true, ignore_errors=true, all_varchar=true)
        WHERE event_name != 'event_name'
          AND user_id_final LIKE 'U%'
    """)

    # --- Query 1: Meta ---
    print("[2/8] Meta stats...")
    meta = con.execute("""
        SELECT
            COUNT(*) AS total_events,
            COUNT(DISTINCT uid) AS total_users,
            MIN(event_date) AS date_from,
            MAX(event_date) AS date_to,
            COUNT(DISTINCT event_date) AS num_days
        FROM events
    """).fetchdf().to_dict(orient="records")[0]
    meta["date_from"] = str(meta["date_from"])[:10]
    meta["date_to"] = str(meta["date_to"])[:10]
    print(f"   Total events: {meta['total_events']:,}, Users: {meta['total_users']:,}")

    # --- Query 2: KPI ---
    print("[3/8] KPI calculations...")
    kpi = con.execute("""
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
            (SELECT ROUND(AVG(EXTRACT(EPOCH FROM (fp.first_purchase_ts - uf.first_touch)) / 3600.0), 1)
             FROM first_purchase fp JOIN user_first uf ON fp.uid = uf.uid
             WHERE uf.first_touch < fp.first_purchase_ts) AS avg_hours_to_first_purchase,
            (SELECT ROUND(AVG(EXTRACT(EPOCH FROM (sp.second_purchase_ts - fp.first_purchase_ts)) / 86400.0), 1)
             FROM first_purchase fp JOIN second_purchase sp ON fp.uid = sp.uid) AS avg_days_to_repeat,
            (SELECT ROUND(100.0 * COUNT(CASE WHEN purchase_count > 1 THEN 1 END) / NULLIF(COUNT(*), 0), 1)
             FROM purchaser_counts) AS repeat_purchase_pct
    """).fetchdf().to_dict(orient="records")[0]

    kpi_data = {
        "total_users": int(meta["total_users"]),
        "conversion_rate": round(100.0 * int(kpi["purchasers"]) / int(meta["total_users"]), 1),
        "purchases": int(kpi["purchasers"]),
        "avg_events_per_user": float(kpi["avg_events_per_user"]),
        "avg_hours_to_first_purchase": float(kpi["avg_hours_to_first_purchase"]) if kpi["avg_hours_to_first_purchase"] else 0,
        "avg_days_to_repeat": float(kpi["avg_days_to_repeat"]) if kpi["avg_days_to_repeat"] else 0,
        "repeat_purchase_pct": float(kpi["repeat_purchase_pct"]) if kpi["repeat_purchase_pct"] else 0,
    }
    print(f"   Conv rate: {kpi_data['conversion_rate']}%, Purchasers: {kpi_data['purchases']:,}")

    # --- Query 3: Funnel ---
    print("[4/8] Funnel counts...")
    funnel_parts = []
    for evt in FUNNEL_EVENTS:
        funnel_parts.append(
            f"COUNT(DISTINCT CASE WHEN event_name = '{evt}' THEN uid END) AS \"{evt}\""
        )
    funnel_sql = f"SELECT {', '.join(funnel_parts)} FROM events"
    funnel_raw = con.execute(funnel_sql).fetchdf().to_dict(orient="records")[0]

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
    print("[5/8] Sankey transitions...")
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
    sankey_raw = con.execute(sankey_sql).fetchdf()

    event_label_map = dict(zip(FUNNEL_EVENTS, FUNNEL_LABELS))
    sankey_nodes = []
    sankey_links = []
    node_set = set()
    for _, row in sankey_raw.iterrows():
        src = event_label_map.get(row["source_event"], row["source_event"])
        tgt = event_label_map.get(row["target_event"], row["target_event"])
        # Layered approach: add step number prefix to avoid cycles
        src_step = FUNNEL_EVENTS.index(row["source_event"]) + 1 if row["source_event"] in FUNNEL_EVENTS else 0
        tgt_step = FUNNEL_EVENTS.index(row["target_event"]) + 1 if row["target_event"] in FUNNEL_EVENTS else 0
        src_label = f"S{src_step}: {src}"
        tgt_label = f"S{tgt_step}: {tgt}"
        if src_label == tgt_label:
            continue  # skip self-loops
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
    print("[6/8] Top user journeys...")
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
        paths AS (
            SELECT uid,
                STRING_AGG(event_name, ' → ' ORDER BY event_ts, step_num) AS path,
                COUNT(*) AS path_len
            FROM (
                SELECT uid, event_name, event_ts, step_num,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_ts, step_num) AS rn
                FROM no_consec
            ) t
            WHERE rn <= 10
            GROUP BY uid
        )
        SELECT path, COUNT(*) AS user_count, path_len
        FROM paths
        GROUP BY path, path_len
        ORDER BY user_count DESC
        LIMIT 20
    """
    journeys_raw = con.execute(journeys_sql).fetchdf()
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
    print("[7/8] Funnel by source...")
    source_parts = []
    for evt in FUNNEL_EVENTS:
        source_parts.append(
            f"COUNT(DISTINCT CASE WHEN event_name = '{evt}' THEN uid END) AS \"{evt}\""
        )
    source_sql = f"""
        SELECT
            {SOURCE_CASE} AS source_group,
            COUNT(DISTINCT uid) AS total_users,
            {', '.join(source_parts)}
        FROM events
        GROUP BY source_group
        ORDER BY total_users DESC
    """
    source_raw = con.execute(source_sql).fetchdf()
    source_data = []
    for _, row in source_raw.iterrows():
        entry = {"source": row["source_group"], "total_users": int(row["total_users"])}
        for i, evt in enumerate(FUNNEL_EVENTS):
            entry[FUNNEL_LABELS[i]] = int(row[evt])
        source_data.append(entry)

    # --- Query 7: Daily Trend ---
    print("[8/8] Daily trend...")
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
    daily_raw = con.execute(daily_sql).fetchdf()
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
    event_dist_raw = con.execute(event_dist_sql).fetchdf()
    event_dist = []
    for _, row in event_dist_raw.iterrows():
        event_dist.append({
            "event": row["event_name"],
            "events": int(row["event_count"]),
            "users": int(row["user_count"]),
        })

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
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)

    elapsed = time.time() - start
    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"\nDone in {elapsed:.1f}s → {OUTPUT_PATH} ({size_kb:.0f} KB)")
    con.close()


if __name__ == "__main__":
    main()
