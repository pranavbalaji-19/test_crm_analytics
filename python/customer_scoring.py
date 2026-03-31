#!/usr/bin/env python3
"""
Script   : customer_scoring.py
Purpose  : Python-based customer scoring pipeline. Builds dynamic SQL queries
           from configurable rule definitions. Uses parameterised segment
           variables ($CUSTOMER_SEGMENT, $RUN_DATE, $BATCH_SIZE) resolved
           from shell arguments. Constructs scoring SQL dynamically from
           feature definitions in the SCORING_FEATURE_CONFIG table.
           Cross-repo: reads DW_OWNER.STG_CUSTOMER_SALES and
                       FINANCE_SCHEMA.FACT_PERIOD_RECONCILIATION.

Called by: process_customer_data.ksh
Args     : --run-date --segment --batch-size --model-version --region
"""

import argparse
import os
import sys
import cx_Oracle
from datetime import datetime, date
from typing import List, Dict, Tuple, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CRM Customer Scoring")
    parser.add_argument("--run-date",       required=True)
    parser.add_argument("--segment",        required=True)
    parser.add_argument("--batch-size",     type=int, default=5000)
    parser.add_argument("--model-version",  default="v2.1")
    parser.add_argument("--region",         default="ALL")
    return parser.parse_args()


def get_connection() -> cx_Oracle.Connection:
    host = os.environ.get("DB_HOST", "oradb-crm-prod.internal.company.com")
    port = int(os.environ.get("DB_PORT", "1521"))
    sid  = os.environ.get("DB_SID",  "CRMDB")
    user = os.environ.get("DB_USER", "CRM_SCHEMA")
    pwd  = os.environ.get("DB_PASS", "")
    dsn  = cx_Oracle.makedsn(host, port, sid=sid)
    return cx_Oracle.connect(user=user, password=pwd, dsn=dsn)


def load_feature_config(conn: cx_Oracle.Connection, model_version: str,
                        segment: str) -> List[Dict]:
    """
    Load dynamic feature definitions from SCORING_FEATURE_CONFIG table.
    Each feature has a name, SQL expression fragment, and weight.
    These are assembled into the scoring SQL at runtime.
    """
    sql = """
        SELECT feature_name,
               sql_expression,
               feature_weight,
               source_table,
               aggregation_window_days
        FROM   SCORING_FEATURE_CONFIG
        WHERE  model_version  = :mv
        AND    (target_segment = :seg OR target_segment = 'ALL')
        AND    is_active       = 'Y'
        ORDER  BY execution_order
    """
    cur = conn.cursor()
    cur.execute(sql, {"mv": model_version, "seg": segment})
    cols = [d[0].lower() for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def build_feature_sql(features: List[Dict], run_date: str,
                      segment: str, region: str) -> Tuple[str, Dict]:
    """
    Dynamically constructs the scoring SELECT from feature config.
    Each feature's sql_expression is a fragment injected into the main SELECT.

    This is the dynamic SQL construction pattern - the entire scoring
    query is assembled from database-driven configuration at runtime.
    """
    if not features:
        # Hardcoded fallback when config table is empty
        features = [
            {"feature_name": "clv_raw",     "sql_expression": "NVL(cs.LIFETIME_VALUE, 0)",
             "feature_weight": 0.4,  "source_table": "DW_OWNER.STG_CUSTOMER_SALES",
             "aggregation_window_days": 365},
            {"feature_name": "recency_days", "sql_expression": "SYSDATE - NVL(cs.LAST_PURCHASE_DATE, SYSDATE - 999)",
             "feature_weight": 0.3,  "source_table": "DW_OWNER.STG_CUSTOMER_SALES",
             "aggregation_window_days": 90},
            {"feature_name": "camp_convs",   "sql_expression": "NVL(ev.conversion_count, 0)",
             "feature_weight": 0.15, "source_table": "STG_CAMPAIGN_EVENTS",
             "aggregation_window_days": 90},
            {"feature_name": "interactions", "sql_expression": "NVL(ix.interaction_count, 0)",
             "feature_weight": 0.15, "source_table": "STG_CUSTOMER_INTERACTIONS",
             "aggregation_window_days": 30},
        ]

    # Build feature SELECT expressions
    feature_selects = []
    for f in features:
        feature_selects.append(f"    ({f['sql_expression']}) AS feat_{f['feature_name']}")

    # Build weighted score expression
    score_parts = []
    for f in features:
        score_parts.append(
            f"({f['feature_weight']} * CASE WHEN ({f['sql_expression']}) IS NULL THEN 0 "
            f"ELSE LEAST(({f['sql_expression']}) / 1000.0, 1.0) END)"
        )

    composite_score = " +\n           ".join(score_parts)

    # Determine region filter
    reg_filter = "" if region == "ALL" else f"AND dc.REGION_CODE = '{region}'"
    seg_filter = "" if segment == "ALL" else f"AND dc.CUSTOMER_SEGMENT = '{segment}'"

    # Full dynamic scoring query
    sql = f"""
        INSERT INTO FACT_CUSTOMER_SCORES (
            SCORE_KEY, CUSTOMER_ID, DIM_CRM_CUSTOMER_KEY,
            SCORE_DATE, CLV_SCORE, CHURN_RISK_SCORE,
            PROPENSITY_BUY_SCORE, ENGAGEMENT_SCORE,
            SEGMENT_CODE, SEGMENT_LABEL,
            BATCH_RUN_DATE, MODEL_VERSION, LOAD_BATCH_ID
        )
        SELECT
            SEQ_FACT_SCORE_KEY.NEXTVAL,
            dc.CUSTOMER_ID,
            dc.DIM_CRM_CUSTOMER_KEY,
            TO_DATE(:run_date, 'YYYY-MM-DD'),

            -- CLV Score: normalised lifetime value (0-10 scale)
            ROUND(LEAST(NVL(cs.LIFETIME_VALUE, 0) / 1000.0, 10.0), 4),

            -- Churn risk (0.0 - 1.0)
            ROUND(
                CASE
                    WHEN cs.LAST_PURCHASE_DATE IS NULL THEN 0.95
                    WHEN SYSDATE - cs.LAST_PURCHASE_DATE > 180 THEN 0.80
                    WHEN SYSDATE - cs.LAST_PURCHASE_DATE > 90  THEN 0.50
                    WHEN SYSDATE - cs.LAST_PURCHASE_DATE > 30  THEN 0.20
                    ELSE 0.05
                END, 4
            ),

            -- Propensity: campaign conversion rate (dynamic from feature config)
            ROUND(NVL(ev.conversion_rate, 0), 4),

            -- Engagement: interaction frequency (dynamic from feature config)
            ROUND(
                CASE
                    WHEN NVL(ix.interaction_count, 0) > 10 THEN 1.0
                    WHEN NVL(ix.interaction_count, 0) > 5  THEN 0.75
                    WHEN NVL(ix.interaction_count, 0) > 0  THEN 0.25
                    ELSE 0.0
                END, 4
            ),

            dc.CUSTOMER_SEGMENT,
            CASE dc.CUSTOMER_SEGMENT
                WHEN 'VIP'       THEN 'High-Value Premium Customer'
                WHEN 'RETAIL'    THEN 'Standard Retail Customer'
                WHEN 'WHOLESALE' THEN 'B2B Wholesale Customer'
                ELSE 'Unclassified'
            END,
            SYSDATE,
            :model_version,
            :batch_id

        FROM DIM_CUSTOMER_CRM dc

        -- Cross-repo: read shared retail staging for lifetime value
        LEFT JOIN DW_OWNER.STG_CUSTOMER_SALES cs
               ON cs.CUSTOMER_ID = dc.CUSTOMER_ID

        -- Campaign conversion rate subquery
        LEFT JOIN (
            SELECT CUSTOMER_ID,
                   COUNT(CASE WHEN EVENT_TYPE = 'CONVERTED' THEN 1 END)
                   / NULLIF(COUNT(*), 0) AS conversion_rate
            FROM   STG_CAMPAIGN_EVENTS
            WHERE  EVENT_DATE >= TO_DATE(:run_date, 'YYYY-MM-DD') - :camp_window
            GROUP  BY CUSTOMER_ID
        ) ev ON ev.CUSTOMER_ID = dc.CUSTOMER_ID

        -- Interaction count subquery
        LEFT JOIN (
            SELECT CUSTOMER_ID,
                   COUNT(*) AS interaction_count
            FROM   STG_CUSTOMER_INTERACTIONS
            WHERE  INTERACTION_DATE >= TO_DATE(:run_date, 'YYYY-MM-DD') - :inter_window
            GROUP  BY CUSTOMER_ID
        ) ix ON ix.CUSTOMER_ID = dc.CUSTOMER_ID

        WHERE dc.IS_CURRENT = 'Y'
        {seg_filter}
        {reg_filter}
        AND NOT EXISTS (
            SELECT 1 FROM FACT_CUSTOMER_SCORES ex
            WHERE  ex.CUSTOMER_ID   = dc.CUSTOMER_ID
            AND    ex.SCORE_DATE    = TO_DATE(:run_date, 'YYYY-MM-DD')
            AND    ex.SEGMENT_CODE  = dc.CUSTOMER_SEGMENT
            AND    ex.MODEL_VERSION = :model_version
        )
        AND ROWNUM <= :batch_size
    """

    # Determine aggregation windows from feature config
    camp_window  = max((f["aggregation_window_days"] for f in features
                        if "campaign" in f.get("source_table","").lower()), default=90)
    inter_window = max((f["aggregation_window_days"] for f in features
                        if "interaction" in f.get("source_table","").lower()), default=30)

    params = {
        "run_date":      run_date,
        "model_version": model_version,
        "batch_id":      int(run_date.replace("-", "")),
        "camp_window":   camp_window,
        "inter_window":  inter_window,
        "batch_size":    5000
    }
    return sql, params


def run_scoring(conn: cx_Oracle.Connection, sql: str, params: Dict) -> int:
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.rowcount
    conn.commit()
    cur.close()
    return rows


def generate_segment_summary(conn: cx_Oracle.Connection, run_date: str,
                              segment: str, region: str) -> None:
    """
    Summarise scored customers into FACT_CUSTOMER_SEGMENT_SUMMARY.
    Dynamic SQL assembled from region/segment parameters.
    """
    seg_filter = "" if segment == "ALL" else f"AND sc.SEGMENT_CODE = '{segment}'"
    reg_filter = "" if region  == "ALL" else f"AND dc.REGION_CODE  = '{region}'"

    summary_sql = f"""
        MERGE INTO FACT_CUSTOMER_SEGMENT_SUMMARY tgt
        USING (
            SELECT sc.SEGMENT_CODE,
                   NVL(dc.REGION_CODE, 'UNKNOWN')           AS REGION_CODE,
                   TO_DATE(:run_date, 'YYYY-MM-DD')          AS SUMMARY_DATE,
                   COUNT(DISTINCT sc.CUSTOMER_ID)            AS CUSTOMER_COUNT,
                   ROUND(AVG(sc.CLV_SCORE), 4)               AS AVG_CLV_SCORE,
                   ROUND(AVG(sc.CHURN_RISK_SCORE), 6)        AS AVG_CHURN_RISK,
                   COUNT(CASE WHEN sc.CLV_SCORE > 5 THEN 1 END)          AS HIGH_VALUE_COUNT,
                   COUNT(CASE WHEN sc.CHURN_RISK_SCORE > 0.7 THEN 1 END) AS AT_RISK_COUNT,
                   NVL(SUM(rs.TOTAL_REVENUE), 0)             AS TOTAL_RETAIL_SPEND
            FROM   FACT_CUSTOMER_SCORES sc
            JOIN   DIM_CUSTOMER_CRM dc ON dc.CUSTOMER_ID = sc.CUSTOMER_ID AND dc.IS_CURRENT = 'Y'
            -- Cross-repo: retail regional summary
            LEFT JOIN DW_OWNER.FACT_REGIONAL_SUMMARY rs
                   ON rs.REGION_CODE  = dc.REGION_CODE
                  AND rs.SUMMARY_DATE = TO_DATE(:run_date, 'YYYY-MM-DD')
            WHERE  sc.SCORE_DATE = TO_DATE(:run_date, 'YYYY-MM-DD')
            {seg_filter}
            {reg_filter}
            GROUP BY sc.SEGMENT_CODE, dc.REGION_CODE
        ) src
        ON (    tgt.SEGMENT_CODE = src.SEGMENT_CODE
            AND tgt.REGION_CODE  = src.REGION_CODE
            AND tgt.SUMMARY_DATE = src.SUMMARY_DATE)
        WHEN MATCHED THEN UPDATE SET
            tgt.CUSTOMER_COUNT   = src.CUSTOMER_COUNT,
            tgt.AVG_CLV_SCORE    = src.AVG_CLV_SCORE,
            tgt.AVG_CHURN_RISK   = src.AVG_CHURN_RISK,
            tgt.HIGH_VALUE_COUNT = src.HIGH_VALUE_COUNT,
            tgt.AT_RISK_COUNT    = src.AT_RISK_COUNT,
            tgt.TOTAL_RETAIL_SPEND = src.TOTAL_RETAIL_SPEND,
            tgt.LOAD_DATE        = SYSDATE
        WHEN NOT MATCHED THEN INSERT (
            SEGMENT_SUMMARY_KEY, SEGMENT_CODE, REGION_CODE, SUMMARY_DATE,
            CUSTOMER_COUNT, AVG_CLV_SCORE, AVG_CHURN_RISK,
            HIGH_VALUE_COUNT, AT_RISK_COUNT, TOTAL_RETAIL_SPEND, LOAD_DATE
        ) VALUES (
            SEQ_SEGMENT_SUMMARY.NEXTVAL, src.SEGMENT_CODE, src.REGION_CODE, src.SUMMARY_DATE,
            src.CUSTOMER_COUNT, src.AVG_CLV_SCORE, src.AVG_CHURN_RISK,
            src.HIGH_VALUE_COUNT, src.AT_RISK_COUNT, src.TOTAL_RETAIL_SPEND, SYSDATE
        )
    """
    cur = conn.cursor()
    cur.execute(summary_sql, {"run_date": run_date})
    conn.commit()
    print(f"[INFO] Segment summary rows merged: {cur.rowcount}")
    cur.close()


def main():
    args = parse_args()
    print(f"[{datetime.now()}] Scoring: date={args.run_date} segment={args.segment} "
          f"batch={args.batch_size} model={args.model_version}")

    conn = get_connection()
    try:
        # Load feature config from DB
        features = load_feature_config(conn, args.model_version, args.segment)
        print(f"[INFO] Loaded {len(features)} features for model={args.model_version}")

        # Build and run scoring SQL
        sql, params = build_feature_sql(features, args.run_date, args.segment, args.region)
        params["batch_size"] = args.batch_size
        rows = run_scoring(conn, sql, params)
        print(f"[INFO] Scores inserted: {rows}")

        # Generate segment summary (reads cross-repo FACT_REGIONAL_SUMMARY)
        generate_segment_summary(conn, args.run_date, args.segment, args.region)

    finally:
        conn.close()

    print(f"[{datetime.now()}] Scoring complete.")


if __name__ == "__main__":
    main()
