-- =============================================================================
-- Ab Initio Graph (.mp) -- Master Program
-- Project  : crm_customer_transform
-- Graph    : crm_customer_scoring.mp
-- Purpose  : Top-level orchestration graph for CRM customer enrichment,
--            scoring, segment aggregation, and churn detection pipeline.
--            Invokes customer_transform.xfr as the core transform logic.
--            Scheduled weekly by UC4 (crm_weekly_workflow.xml).
--
-- Cross-repo dependencies:
--   - DW_OWNER.STG_CUSTOMER_SALES  (from retail_dw_legacy)
--   - DW_OWNER.FACT_REGIONAL_SUMMARY (from retail_dw_legacy)
--   - FINANCE_SCHEMA.FACT_PERIOD_RECONCILIATION (from finance_etl_legacy)
--
-- Triggered after:
--   - FINANCE_GL_CLOSE_COMPLETE event
--   - RETAIL_DAILY_COMPLETE event
-- =============================================================================

graph crm_customer_scoring;

parameters
  -- Runtime parameters (passed from process_customer_data.ksh / UC4)
  string RUN_DATE              = "${RUN_DATE}";
  string CUSTOMER_SEGMENT      = "${CUSTOMER_SEGMENT}";
  string BATCH_SIZE            = "${BATCH_SIZE:-5000}";

  -- Connection parameters
  string CRM_ORA_CONNECT       = "${CRM_ORA_CONNECT}";

  -- Schema files
  string SCHEMA_FILE           = "crm_schema.dml";
  string TRANSFORM_FILE        = "customer_transform.xfr";

  -- Output paths
  string AT_RISK_OUTPUT_DIR    = "/opt/etl/crm";
  string ERROR_OUTPUT_DIR      = "/opt/etl/errors/crm";
  string AUDIT_LOG_DIR         = "/opt/etl/logs/crm";

  -- Parallelism
  integer AB_PARALLELISM       = 4;

  -- Thresholds
  decimal CHURN_RISK_THRESHOLD = 0.70;
  integer MIN_ROW_COUNT        = 10;
end;


-- ==========================================================================
-- Phase 1: Source Reads (parallel)
-- ==========================================================================

-- Read CRM customer profiles from local staging
read_crm_customers: input_table
  database   = CRM_ORA_CONNECT;
  table      = "STG_CUSTOMER_PROFILE";
  query      = "SELECT * FROM STG_CUSTOMER_PROFILE "
               "WHERE LOAD_DATE = TO_DATE('" || RUN_DATE || "','YYYY-MM-DD') "
               "AND CUSTOMER_SEGMENT = '" || CUSTOMER_SEGMENT || "' "
               "AND ETL_STATUS = 'PENDING'";
  record_format = SCHEMA_FILE : "stg_customer_profile";
  fetch_size = BATCH_SIZE;
end;

-- Read retail spend from shared DW_OWNER staging (cross-repo)
read_retail_spend: input_table
  database   = CRM_ORA_CONNECT;
  table      = "DW_OWNER.STG_CUSTOMER_SALES";
  query      = "SELECT customer_id, lifetime_value, loyalty_tier, "
               "last_purchase_date, region_code "
               "FROM DW_OWNER.STG_CUSTOMER_SALES "
               "WHERE LOAD_DATE >= TO_DATE('" || RUN_DATE || "','YYYY-MM-DD') - 7";
  record_format = SCHEMA_FILE : "retail_spend_record";
end;

-- Read campaign events for current run date
read_campaign_events: input_table
  database   = CRM_ORA_CONNECT;
  table      = "STG_CAMPAIGN_EVENTS";
  query      = "SELECT * FROM STG_CAMPAIGN_EVENTS "
               "WHERE LOAD_DATE = TO_DATE('" || RUN_DATE || "','YYYY-MM-DD')";
  record_format = SCHEMA_FILE : "campaign_event_record";
end;

-- Read regional summary from shared DW_OWNER (cross-repo, for validation)
read_regional_summary: input_table
  database   = CRM_ORA_CONNECT;
  table      = "DW_OWNER.FACT_REGIONAL_SUMMARY";
  query      = "SELECT region_code, SUM(total_revenue) AS total_regional_revenue "
               "FROM DW_OWNER.FACT_REGIONAL_SUMMARY "
               "WHERE summary_date >= TO_DATE('" || RUN_DATE || "','YYYY-MM-DD') - 30 "
               "GROUP BY region_code";
  record_format = SCHEMA_FILE : "retail_spend_record";
end;


-- ==========================================================================
-- Phase 2: Core Transformation (references customer_transform.xfr)
-- ==========================================================================

-- Campaign rollup to customer level
rollup_campaigns: rollup
  input      = read_campaign_events;
  keys       = "customer_id";
  record_format = SCHEMA_FILE : "campaign_event_record";
  transform  = <<CAMP_ROLLUP
    initialise:
      out.customer_id            = in.customer_id;
      out.campaign_conversions   = 0;
      out.total_campaign_revenue = 0.0;
    aggregate:
      out.campaign_conversions   += (in.event_type EQ 'CONVERTED') ? 1 : 0;
      out.total_campaign_revenue += in.revenue_attributed;
  CAMP_ROLLUP
end;

-- Join customer profiles with retail spend (LEFT OUTER)
join_with_retail: join
  input[0]     = read_crm_customers;
  input[1]     = read_retail_spend;
  keys         = "customer_id";
  join_type    = "left_outer";
  record_format = SCHEMA_FILE : "enriched_customer_record";
end;

-- Join with campaign rollup (LEFT OUTER)
join_with_campaigns: join
  input[0]     = join_with_retail;
  input[1]     = rollup_campaigns;
  keys         = "customer_id";
  join_type    = "left_outer";
  record_format = SCHEMA_FILE : "enriched_customer_record";
end;

-- Core scoring transform (references external XFR)
score_customers: reformat
  input      = join_with_campaigns;
  transform  = TRANSFORM_FILE;
  record_format = SCHEMA_FILE : "enriched_customer_record";
end;

-- Split by churn risk threshold
filter_churn: filter_by_expression
  input      = score_customers;
  keep       = "churn_risk_score <= " || string(CHURN_RISK_THRESHOLD);
  reject     = "churn_risk_score > " || string(CHURN_RISK_THRESHOLD);
end;

-- Segment-level aggregation
rollup_segments: rollup
  input      = score_customers;
  keys       = "customer_segment, region_code";
  record_format = SCHEMA_FILE : "segment_aggregate_record";
  transform  = <<SEG_ROLLUP
    initialise:
      out.segment_code         = in.customer_segment;
      out.region_code          = in.region_code;
      out.summary_date         = date_from_string(RUN_DATE, 'YYYY-MM-DD');
      out.customer_count       = 0;
      out.avg_clv_score        = 0.0;
      out.avg_churn_risk       = 0.0;
      out.high_value_count     = 0;
      out.at_risk_count        = 0;
      out.opted_in_email_count = 0;
      out.total_retail_spend   = 0.0;
    aggregate:
      out.customer_count       += 1;
      out.avg_clv_score        += in.clv_score;
      out.avg_churn_risk       += in.churn_risk_score;
      out.high_value_count     += (in.clv_score > 5.0)          ? 1 : 0;
      out.at_risk_count        += (in.churn_risk_score > 0.7)   ? 1 : 0;
      out.opted_in_email_count += (in.is_opted_in_email EQ 'Y') ? 1 : 0;
      out.total_retail_spend   += in.lifetime_value;
    finalise:
      out.avg_clv_score  = out.avg_clv_score  / max(out.customer_count, 1);
      out.avg_churn_risk = out.avg_churn_risk / max(out.customer_count, 1);
  SEG_ROLLUP
end;


-- ==========================================================================
-- Phase 3: Outputs
-- ==========================================================================

-- Write scored customers to FACT_CUSTOMER_SCORES
write_scores: output_table
  input      = score_customers;
  database   = CRM_ORA_CONNECT;
  table      = "FACT_CUSTOMER_SCORES";
  mode       = "insert_append";
end;

-- Write segment summary to FACT_CUSTOMER_SEGMENT_SUMMARY
write_segment_summary: output_table
  input      = rollup_segments;
  database   = CRM_ORA_CONNECT;
  table      = "FACT_CUSTOMER_SEGMENT_SUMMARY";
  mode       = "insert_append";
end;

-- Write at-risk customers to intervention file
write_at_risk_file: output_file
  input      = filter_churn.reject;
  filename   = AT_RISK_OUTPUT_DIR || "/at_risk_" || CUSTOMER_SEGMENT || "_" || RUN_DATE || ".dat";
  record_format = SCHEMA_FILE : "enriched_customer_record";
  delimiter  = "|";
end;

-- Write row count audit log
write_audit_log: output_file
  input      = score_customers;
  filename   = AUDIT_LOG_DIR || "/crm_scoring_audit_" || RUN_DATE || ".log";
  mode       = "count_only";
end;


-- ==========================================================================
-- Flow Definition
-- ==========================================================================

-- Phase 1: parallel reads
read_crm_customers, read_retail_spend, read_campaign_events, read_regional_summary;

-- Phase 2: transforms
read_campaign_events    -> rollup_campaigns;
read_crm_customers      -> join_with_retail;
read_retail_spend       -> join_with_retail;
join_with_retail        -> join_with_campaigns;
rollup_campaigns        -> join_with_campaigns;
join_with_campaigns     -> score_customers;
score_customers         -> filter_churn;
score_customers         -> rollup_segments;

-- Phase 3: outputs (parallel writes)
score_customers         -> write_scores;
rollup_segments         -> write_segment_summary;
filter_churn.reject     -> write_at_risk_file;
score_customers         -> write_audit_log;

end;
