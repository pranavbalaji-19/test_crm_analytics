#!/usr/bin/env python3
"""
Script   : crm_lineage_tracker.py
Purpose  : Tracks and documents source-to-target data lineage across all
           3 repos (retail_dw_legacy, finance_etl_legacy, crm_analytics_legacy).
           Queries Oracle system catalogs and cross-schema objects to build
           an end-to-end lineage graph. Outputs lineage JSON for the
           DataStreak Discovery Engine to consume (REQ-WG-02).

           Lineage chain documented:
           UC4 Trigger -> DIL Source -> ksh Script -> Oracle Staging Table
           -> PL/SQL Job -> Target Analytical Table

Called by: CRM_WEEKLY_WORKFLOW / CRM_PYTHON_LINEAGE
Args     : --run-date --workflow --output-dir
"""

import argparse
import os
import json
import sys
import cx_Oracle
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import defaultdict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CRM Lineage Tracker")
    parser.add_argument("--run-date",    required=True)
    parser.add_argument("--workflow",    default="CRM_WEEKLY_WORKFLOW")
    parser.add_argument("--output-dir",  default="/opt/etl/lineage/crm")
    parser.add_argument("--schemas",     default="CRM_SCHEMA,DW_OWNER,FINANCE_SCHEMA",
                        help="Comma-separated list of schemas to trace")
    return parser.parse_args()


def get_connection() -> cx_Oracle.Connection:
    host = os.environ.get("DB_HOST", "oradb-crm-prod.internal.company.com")
    port = int(os.environ.get("DB_PORT", "1521"))
    sid  = os.environ.get("DB_SID",  "CRMDB")
    user = os.environ.get("DB_USER", "CRM_SCHEMA")
    pwd  = os.environ.get("DB_PASS", "")
    dsn  = cx_Oracle.makedsn(host, port, sid=sid)
    return cx_Oracle.connect(user=user, password=pwd, dsn=dsn)


def extract_table_dependencies(conn: cx_Oracle.Connection,
                                schemas: List[str]) -> List[Dict]:
    """
    Query Oracle DBA_DEPENDENCIES to find PL/SQL object dependencies on tables.
    This maps: package/procedure -> source tables -> target tables.
    """
    schema_list = ", ".join(f"'{s}'" for s in schemas)
    sql = f"""
        SELECT d.owner        AS object_owner,
               d.name         AS object_name,
               d.type         AS object_type,
               d.referenced_owner AS ref_owner,
               d.referenced_name  AS ref_name,
               d.referenced_type  AS ref_type
        FROM   DBA_DEPENDENCIES d
        WHERE  d.owner IN ({schema_list})
        AND    d.referenced_type IN ('TABLE','VIEW','SYNONYM')
        AND    d.referenced_owner IN ({schema_list})
        AND    d.type IN ('PACKAGE BODY','PROCEDURE','FUNCTION','TRIGGER')
        ORDER  BY d.owner, d.name
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except cx_Oracle.Error as e:
        # Fallback if DBA_DEPENDENCIES not accessible
        print(f"[WARN] DBA_DEPENDENCIES not accessible: {e}. Using ALL_DEPENDENCIES.")
        schema_list_all = ", ".join(f"'{s}'" for s in schemas)
        sql_all = f"""
            SELECT owner AS object_owner, name AS object_name, type AS object_type,
                   referenced_owner AS ref_owner, referenced_name AS ref_name,
                   referenced_type AS ref_type
            FROM   ALL_DEPENDENCIES
            WHERE  owner IN ({schema_list_all})
            AND    referenced_type IN ('TABLE','VIEW','SYNONYM')
            ORDER  BY owner, name
        """
        cur.execute(sql_all)
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()


def extract_cross_schema_grants(conn: cx_Oracle.Connection,
                                 schemas: List[str]) -> List[Dict]:
    """
    Find cross-schema table grants that indicate data flow between repos.
    e.g. DW_OWNER grants SELECT ON STG_CUSTOMER_SALES TO CRM_SCHEMA
         => retail_dw_legacy feeds crm_analytics_legacy.
    """
    grantee_list = ", ".join(f"'{s}'" for s in schemas)
    owner_list   = ", ".join(f"'{s}'" for s in schemas)
    sql = f"""
        SELECT tp.owner         AS source_schema,
               tp.table_name    AS source_table,
               tp.grantee       AS target_schema,
               tp.privilege     AS privilege
        FROM   ALL_TAB_PRIVS tp
        WHERE  tp.owner   IN ({owner_list})
        AND    tp.grantee IN ({grantee_list})
        AND    tp.owner  <> tp.grantee
        AND    tp.privilege IN ('SELECT','INSERT','UPDATE','DELETE')
        ORDER  BY tp.owner, tp.table_name, tp.grantee
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except cx_Oracle.Error as e:
        print(f"[WARN] Could not query cross-schema grants: {e}")
        return []
    finally:
        cur.close()


def extract_etl_job_audit(conn: cx_Oracle.Connection, run_date: str) -> List[Dict]:
    """
    Pull ETL job audit records for the run date to determine execution order
    and actual source/target row counts for lineage documentation.
    """
    sql = """
        SELECT workflow_name, job_name, run_date,
               status, rows_processed, created_date, updated_date
        FROM   ETL_JOB_AUDIT
        WHERE  run_date = TO_DATE(:run_date, 'YYYY-MM-DD')
        ORDER  BY created_date
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, {"run_date": run_date})
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except cx_Oracle.Error:
        return []  # Audit table may not exist in all environments
    finally:
        cur.close()


def build_lineage_graph(dependencies: List[Dict],
                        cross_grants: List[Dict],
                        audit_records: List[Dict],
                        run_date: str,
                        workflow: str) -> Dict:
    """
    Build end-to-end lineage graph connecting:
    UC4 Job -> ksh Script -> Oracle Staging -> PL/SQL Package -> Fact Table

    This is the REQ-WG-02 lineage map format.
    """
    # Known workflow -> script -> staging -> PL/SQL -> target mappings
    # (hard-coded topology derived from code analysis)
    lineage_chains = [
        {
            "chain_id": "CRM-001",
            "uc4_workflow":     "CRM_WEEKLY_WORKFLOW",
            "uc4_job":          "CRM_CUSTOMER_EXTRACT_VIP",
            "ksh_script":       "process_customer_data.ksh",
            "sqlplus_script":   "customer_segment_extract.sql",
            "source_tables":    ["SOURCE_CRM.CUSTOMERS", "SOURCE_CRM.CUSTOMER_SEGMENTS"],
            "staging_table":    "CRM_SCHEMA.STG_CUSTOMER_PROFILE",
            "plsql_package":    "CRM_SCHEMA.PKG_CUSTOMER_HISTORIZATION.LOAD_DIM_CUSTOMER_CRM",
            "target_table":     "CRM_SCHEMA.DIM_CUSTOMER_CRM",
            "segment_code":     "VIP"
        },
        {
            "chain_id": "CRM-002",
            "uc4_workflow":     "CRM_WEEKLY_WORKFLOW",
            "uc4_job":          "CRM_ABINITIO_TRANSFORM",
            "abinitio_graph":   "customer_transform.xfr",
            "source_tables":    ["CRM_SCHEMA.STG_CUSTOMER_PROFILE",
                                 "DW_OWNER.STG_CUSTOMER_SALES"],  # cross-repo
            "staging_table":    None,
            "plsql_package":    None,
            "target_table":     "CRM_SCHEMA.FACT_CUSTOMER_SCORES",
            "cross_repo_source": "retail_dw_legacy"
        },
        {
            "chain_id": "CRM-003",
            "uc4_workflow":     "CRM_WEEKLY_WORKFLOW",
            "uc4_job":          "CRM_SPARK_SEGMENTATION",
            "spark_job":        "customer_segmentation.scala",
            "source_tables":    ["CRM_SCHEMA.FACT_CUSTOMER_SCORES",
                                 "DW_OWNER.STG_CUSTOMER_SALES",    # cross-repo: retail
                                 "DW_OWNER.FACT_REGIONAL_SUMMARY"],# cross-repo: retail
            "staging_table":    None,
            "target_table":     "CRM_SCHEMA.FACT_CUSTOMER_SEGMENT_SUMMARY",
            "cross_repo_source": "retail_dw_legacy"
        },
        {
            "chain_id": "CRM-004",
            "uc4_workflow":     "CRM_WEEKLY_WORKFLOW",
            "uc4_job":          "CRM_CUSTOMER_EXTRACT_RETAIL",
            "ksh_script":       "process_customer_data.ksh",
            "source_tables":    ["SOURCE_CRM.CAMPAIGN_EVENTS", "SOURCE_CRM.CAMPAIGNS"],
            "staging_table":    "CRM_SCHEMA.STG_CAMPAIGN_EVENTS",
            "plsql_package":    "CRM_SCHEMA.PKG_CUSTOMER_HISTORIZATION.AGGREGATE_CAMPAIGN_PERFORMANCE",
            "target_table":     "CRM_SCHEMA.FACT_CAMPAIGN_PERFORMANCE",
            "segment_code":     "RETAIL"
        },
    ]

    # Add cross-repo lineage links from grants
    cross_repo_links = []
    for grant in cross_grants:
        cross_repo_links.append({
            "source_repo":  _schema_to_repo(grant["source_schema"]),
            "source_schema": grant["source_schema"],
            "source_table": grant["source_table"],
            "target_repo":  _schema_to_repo(grant["target_schema"]),
            "target_schema": grant["target_schema"],
            "access_type":  grant["privilege"],
            "link_type":    "CROSS_REPO_GRANT"
        })

    # Enrich chains with dependency data
    dep_map = defaultdict(list)
    for dep in dependencies:
        dep_map[f"{dep['object_owner']}.{dep['object_name']}"].append(
            f"{dep['ref_owner']}.{dep['ref_name']}"
        )

    for chain in lineage_chains:
        pkg = chain.get("plsql_package")
        if pkg:
            # Find tables referenced by this package from DBA_DEPENDENCIES
            pkg_base = ".".join(pkg.split(".")[:2])  # SCHEMA.PACKAGE_NAME
            chain["plsql_table_refs"] = dep_map.get(pkg_base, [])

    return {
        "lineage_snapshot": {
            "run_date":     run_date,
            "workflow":     workflow,
            "generated_at": datetime.now().isoformat(),
            "schema_scope": ["CRM_SCHEMA", "DW_OWNER", "FINANCE_SCHEMA"],
        },
        "lineage_chains":     lineage_chains,
        "cross_repo_links":   cross_repo_links,
        "dependency_map":     dict(dep_map),
        "etl_execution_log":  audit_records,
        "summary": {
            "total_chains":         len(lineage_chains),
            "cross_repo_links":     len(cross_repo_links),
            "plsql_dependencies":   len(dependencies),
            "repos_involved":       ["crm_analytics_legacy", "retail_dw_legacy", "finance_etl_legacy"]
        }
    }


def _schema_to_repo(schema: str) -> str:
    mapping = {
        "CRM_SCHEMA":     "crm_analytics_legacy",
        "DW_OWNER":       "retail_dw_legacy",
        "FINANCE_SCHEMA": "finance_etl_legacy",
    }
    return mapping.get(schema.upper(), "unknown")


def main():
    args = parse_args()
    schemas = [s.strip().upper() for s in args.schemas.split(",")]

    print(f"[{datetime.now()}] CRM Lineage Tracker: date={args.run_date} "
          f"workflow={args.workflow} schemas={schemas}")

    conn = get_connection()
    try:
        print("[INFO] Extracting table dependencies from Oracle catalog...")
        dependencies = extract_table_dependencies(conn, schemas)
        print(f"[INFO] Found {len(dependencies)} PL/SQL -> table dependencies")

        print("[INFO] Extracting cross-schema grants...")
        cross_grants = extract_cross_schema_grants(conn, schemas)
        print(f"[INFO] Found {len(cross_grants)} cross-schema grants")

        print("[INFO] Loading ETL job audit records...")
        audit_records = extract_etl_job_audit(conn, args.run_date)
        print(f"[INFO] Found {len(audit_records)} audit records")

        lineage_graph = build_lineage_graph(
            dependencies, cross_grants, audit_records,
            args.run_date, args.workflow
        )

        # Write lineage output
        os.makedirs(args.output_dir, exist_ok=True)
        out_file = os.path.join(
            args.output_dir,
            f"lineage_{args.workflow}_{args.run_date.replace('-','')}.json"
        )
        with open(out_file, "w") as f:
            json.dump(lineage_graph, f, indent=2, default=str)

        print(f"[INFO] Lineage graph written to: {out_file}")
        print(f"[INFO] Summary: {lineage_graph['summary']}")

    finally:
        conn.close()

    print(f"[{datetime.now()}] Lineage tracking complete.")


if __name__ == "__main__":
    main()
