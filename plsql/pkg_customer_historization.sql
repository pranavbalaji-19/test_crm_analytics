-- =============================================================================
-- Package: PKG_CUSTOMER_HISTORIZATION
-- Description: SCD Type 2 historization for DIM_CUSTOMER_CRM.
--              Loads customer scores, campaign performance aggregation.
--              HIGH COMPLEXITY: nested SCD logic, EXECUTE IMMEDIATE for
--              segment-based dynamic partitioning, multi-level nested PL/SQL
--              blocks with dynamic SQL built from config table rules.
-- Schema: CRM_SCHEMA
-- =============================================================================

CREATE OR REPLACE PACKAGE PKG_CUSTOMER_HISTORIZATION AS

    PROCEDURE LOAD_DIM_CUSTOMER_CRM(
        p_run_date        IN DATE,
        p_segment_code    IN VARCHAR2 DEFAULT 'ALL',
        p_region_code     IN VARCHAR2 DEFAULT 'ALL',
        p_rows_loaded     OUT NUMBER
    );

    PROCEDURE LOAD_FACT_CUSTOMER_SCORES(
        p_score_date      IN DATE,
        p_segment_code    IN VARCHAR2,
        p_model_version   IN VARCHAR2 DEFAULT 'v2.1',
        p_batch_id        IN NUMBER
    );

    PROCEDURE AGGREGATE_CAMPAIGN_PERFORMANCE(
        p_run_date        IN DATE,
        p_campaign_id     IN VARCHAR2 DEFAULT 'ALL'
    );

    PROCEDURE GENERATE_SEGMENT_SUMMARY(
        p_summary_date    IN DATE,
        p_region_code     IN VARCHAR2 DEFAULT 'ALL'
    );

    PROCEDURE MASTER_CRM_LOAD(
        p_run_date        IN DATE,
        p_segment_code    IN VARCHAR2,
        p_region_code     IN VARCHAR2
    );

END PKG_CUSTOMER_HISTORIZATION;
/

CREATE OR REPLACE PACKAGE BODY PKG_CUSTOMER_HISTORIZATION AS

    -- -------------------------------------------------------------------------
    -- PROC: LOAD_DIM_CUSTOMER_CRM
    -- SCD Type 2 with nested dynamic SQL for segment-specific processing.
    -- Reads from shared STG_CUSTOMER_SALES (DW_OWNER) + local STG_CUSTOMER_PROFILE.
    -- -------------------------------------------------------------------------
    PROCEDURE LOAD_DIM_CUSTOMER_CRM(
        p_run_date        IN DATE,
        p_segment_code    IN VARCHAR2 DEFAULT 'ALL',
        p_region_code     IN VARCHAR2 DEFAULT 'ALL',
        p_rows_loaded     OUT NUMBER
    ) IS
        v_sql           VARCHAR2(8000);
        v_seg_filter    VARCHAR2(200);
        v_reg_filter    VARCHAR2(200);
        v_stg_table     VARCHAR2(100);
        v_rows          NUMBER := 0;
        v_new           NUMBER := 0;
        v_changed       NUMBER := 0;
        v_batch_id      NUMBER;

        -- Cursor over segments to process (supports ALL or specific segment)
        TYPE t_seg_tab IS TABLE OF VARCHAR2(30);
        v_segments t_seg_tab;
        v_cur SYS_REFCURSOR;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('>>> LOAD_DIM_CUSTOMER_CRM: date=' || TO_CHAR(p_run_date,'YYYY-MM-DD')
            || ' segment=' || p_segment_code || ' region=' || p_region_code);

        v_batch_id := SEQ_CRM_BATCH_ID.NEXTVAL;

        -- Build dynamic filter clauses
        v_seg_filter := CASE UPPER(p_segment_code)
            WHEN 'ALL' THEN '1=1'
            ELSE 'CUSTOMER_SEGMENT = ''' || UPPER(p_segment_code) || ''''
        END;

        v_reg_filter := CASE UPPER(p_region_code)
            WHEN 'ALL' THEN '1=1'
            ELSE 'REGION_CODE = ''' || UPPER(p_region_code) || ''''
        END;

        -- Merge enriched profile from local staging + shared retail staging
        -- NOTE: DW_OWNER.STG_CUSTOMER_SALES is the cross-repo shared table
        v_sql :=
            'MERGE INTO DIM_CUSTOMER_CRM tgt '
         || 'USING ( '
         || '    SELECT cp.CUSTOMER_ID, '
         || '           cp.CUSTOMER_CODE, '
         || '           cp.FIRST_NAME || '' '' || cp.LAST_NAME  AS FULL_NAME, '
         || '           cp.EMAIL, '
         || '           cp.PHONE, '
         || '           cp.DATE_OF_BIRTH, '
         || '           cp.POSTCODE, '
         || '           cp.REGION_CODE, '
         || '           cp.CUSTOMER_SEGMENT, '
         || '           cp.ACQUISITION_CHANNEL, '
         || '           cp.IS_OPTED_IN_EMAIL, '
         || '           NVL(cs.LOYALTY_TIER, ''STANDARD'')     AS LOYALTY_TIER, '
         || '           NVL(cs.LIFETIME_VALUE, 0)              AS LIFETIME_VALUE '
         || '    FROM   STG_CUSTOMER_PROFILE cp '
         || '    LEFT JOIN DW_OWNER.STG_CUSTOMER_SALES cs '    -- cross-schema read
         ||       'ON cs.CUSTOMER_ID = cp.CUSTOMER_ID '
         || '    WHERE  cp.LOAD_DATE    = :run_dt '
         || '    AND    cp.ETL_STATUS   = ''PENDING'' '
         || '    AND    ' || v_seg_filter
         || '    AND    ' || v_reg_filter
         || ') src '
         || 'ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = ''Y'') '
         || 'WHEN MATCHED THEN UPDATE SET '
         || '    tgt.VALID_TO       = :run_dt - 1, '
         || '    tgt.IS_CURRENT     = ''N'', '
         || '    tgt.UPDATED_DATE   = SYSDATE '
         || 'WHERE ( '
         || '    NVL(tgt.FULL_NAME,        ''X'') <> NVL(src.FULL_NAME,        ''X'') OR '
         || '    NVL(tgt.EMAIL,            ''X'') <> NVL(src.EMAIL,            ''X'') OR '
         || '    NVL(tgt.POSTCODE,         ''X'') <> NVL(src.POSTCODE,         ''X'') OR '
         || '    NVL(tgt.CUSTOMER_SEGMENT, ''X'') <> NVL(src.CUSTOMER_SEGMENT, ''X'') OR '
         || '    NVL(tgt.IS_OPTED_IN_EMAIL,''X'') <> NVL(src.IS_OPTED_IN_EMAIL,''X'') '
         || ')';

        EXECUTE IMMEDIATE v_sql USING p_run_date, p_run_date;
        v_changed := SQL%ROWCOUNT;

        -- Insert new current versions for both new customers and expired/changed ones
        INSERT INTO DIM_CUSTOMER_CRM (
            DIM_CRM_CUSTOMER_KEY, CUSTOMER_ID, CUSTOMER_CODE, FULL_NAME,
            EMAIL, PHONE, DATE_OF_BIRTH, POSTCODE,
            REGION_CODE, CUSTOMER_SEGMENT, ACQUISITION_CHANNEL,
            IS_OPTED_IN_EMAIL,
            VALID_FROM, VALID_TO, IS_CURRENT, VERSION_NUM, CREATED_DATE
        )
        SELECT SEQ_DIM_CRM_CUST_KEY.NEXTVAL,
               cp.CUSTOMER_ID,
               cp.CUSTOMER_CODE,
               cp.FIRST_NAME || ' ' || cp.LAST_NAME,
               cp.EMAIL, cp.PHONE, cp.DATE_OF_BIRTH, cp.POSTCODE,
               cp.REGION_CODE, cp.CUSTOMER_SEGMENT, cp.ACQUISITION_CHANNEL,
               cp.IS_OPTED_IN_EMAIL,
               p_run_date,
               TO_DATE('9999-12-31','YYYY-MM-DD'),
               'Y',
               NVL((SELECT MAX(d2.VERSION_NUM) FROM DIM_CUSTOMER_CRM d2
                    WHERE d2.CUSTOMER_ID = cp.CUSTOMER_ID), 0) + 1,
               SYSDATE
        FROM   STG_CUSTOMER_PROFILE cp
        WHERE  cp.LOAD_DATE    = p_run_date
        AND    cp.ETL_STATUS   = 'PENDING'
        AND    NOT EXISTS (
               SELECT 1 FROM DIM_CUSTOMER_CRM d
               WHERE  d.CUSTOMER_ID = cp.CUSTOMER_ID
               AND    d.IS_CURRENT  = 'Y'
        );

        v_new := SQL%ROWCOUNT;

        -- Mark staging as processed
        UPDATE STG_CUSTOMER_PROFILE
        SET    ETL_STATUS = 'LOADED'
        WHERE  LOAD_DATE  = p_run_date
        AND    ETL_STATUS = 'PENDING';

        COMMIT;
        p_rows_loaded := v_new + v_changed;
        DBMS_OUTPUT.PUT_LINE('<<< LOAD_DIM_CUSTOMER_CRM: new=' || v_new || ' changed=' || v_changed);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('ERROR in LOAD_DIM_CUSTOMER_CRM: ' || SQLERRM);
            RAISE;
    END LOAD_DIM_CUSTOMER_CRM;

    -- -------------------------------------------------------------------------
    -- PROC: LOAD_FACT_CUSTOMER_SCORES
    -- Generates scoring records per customer for a given segment.
    -- Uses EXECUTE IMMEDIATE with dynamic segment table for score inputs.
    -- -------------------------------------------------------------------------
    PROCEDURE LOAD_FACT_CUSTOMER_SCORES(
        p_score_date      IN DATE,
        p_segment_code    IN VARCHAR2,
        p_model_version   IN VARCHAR2 DEFAULT 'v2.1',
        p_batch_id        IN NUMBER
    ) IS
        v_sql           VARCHAR2(6000);
        v_score_src_tbl VARCHAR2(100);
        v_seg_tbl_exists NUMBER;
        v_rows          NUMBER;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('>>> LOAD_FACT_CUSTOMER_SCORES: segment=' || p_segment_code
            || ' date=' || TO_CHAR(p_score_date,'YYYY-MM-DD'));

        -- Check if segment has a dedicated score input table (e.g. SCORE_INPUT_VIP)
        v_score_src_tbl := 'SCORE_INPUT_' || UPPER(REPLACE(p_segment_code,'-','_'));
        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM ALL_TABLES '
         || 'WHERE TABLE_NAME = :t AND OWNER = ''CRM_SCHEMA'''
        INTO v_seg_tbl_exists
        USING v_score_src_tbl;

        IF v_seg_tbl_exists = 0 THEN
            -- Fall back to generic score input view
            v_score_src_tbl := 'V_SCORE_INPUTS_GENERIC';
            DBMS_OUTPUT.PUT_LINE('  Using generic score source: ' || v_score_src_tbl);
        ELSE
            DBMS_OUTPUT.PUT_LINE('  Using segment-specific score source: ' || v_score_src_tbl);
        END IF;

        -- Delete existing scores for this date/segment (idempotent)
        EXECUTE IMMEDIATE
            'DELETE FROM FACT_CUSTOMER_SCORES '
         || 'WHERE SCORE_DATE      = :dt '
         || '  AND SEGMENT_CODE    = :seg '
         || '  AND MODEL_VERSION   = :ver'
        USING p_score_date, p_segment_code, p_model_version;

        -- Insert new scores - complex multi-source join including cross-repo tables
        v_sql :=
            'INSERT INTO FACT_CUSTOMER_SCORES ( '
         || '    SCORE_KEY, CUSTOMER_ID, DIM_CRM_CUSTOMER_KEY, '
         || '    SCORE_DATE, '
         || '    CLV_SCORE, CHURN_RISK_SCORE, PROPENSITY_BUY_SCORE, '
         || '    ENGAGEMENT_SCORE, '
         || '    SEGMENT_CODE, SEGMENT_LABEL, '
         || '    BATCH_RUN_DATE, MODEL_VERSION, LOAD_BATCH_ID '
         || ') '
         || 'SELECT '
         || '    SEQ_FACT_SCORE_KEY.NEXTVAL, '
         || '    d.CUSTOMER_ID, '
         || '    d.DIM_CRM_CUSTOMER_KEY, '
         || '    :score_dt, '
         || '    -- CLV score: based on lifetime value from shared retail staging '
         || '    ROUND(NVL(cs.LIFETIME_VALUE, 0) / 1000, 4)  AS CLV_SCORE, '
         || '    -- Churn risk: recency-based from interaction data '
         || '    CASE '
         || '        WHEN cs.LAST_PURCHASE_DATE IS NULL THEN 0.95 '
         || '        WHEN SYSDATE - cs.LAST_PURCHASE_DATE > 180 THEN 0.80 '
         || '        WHEN SYSDATE - cs.LAST_PURCHASE_DATE > 90  THEN 0.50 '
         || '        WHEN SYSDATE - cs.LAST_PURCHASE_DATE > 30  THEN 0.20 '
         || '        ELSE 0.05 '
         || '    END AS CHURN_RISK_SCORE, '
         || '    -- Propensity: campaign response rate '
         || '    ROUND(NVL(camp.CONVERSION_RATE, 0), 4)      AS PROPENSITY_BUY_SCORE, '
         || '    -- Engagement: interaction frequency score '
         || '    ROUND(NVL(inter.INTERACT_SCORE, 0), 4)       AS ENGAGEMENT_SCORE, '
         || '    d.CUSTOMER_SEGMENT, '
         || '    CASE d.CUSTOMER_SEGMENT '
         || '        WHEN ''VIP''       THEN ''High-Value Premium'' '
         || '        WHEN ''RETAIL''    THEN ''Standard Retail'' '
         || '        WHEN ''WHOLESALE'' THEN ''B2B Wholesale'' '
         || '        ELSE ''Unclassified'' '
         || '    END AS SEGMENT_LABEL, '
         || '    SYSDATE, :model_ver, :batch '
         || 'FROM DIM_CUSTOMER_CRM d '
         || 'LEFT JOIN DW_OWNER.STG_CUSTOMER_SALES cs '       -- cross-repo join
         ||     'ON cs.CUSTOMER_ID = d.CUSTOMER_ID '
         || 'LEFT JOIN ( '
         ||     'SELECT CUSTOMER_ID, '
         ||            'SUM(CASE WHEN EVENT_TYPE=''CONVERTED'' THEN 1 ELSE 0 END) '
         ||            '/ NULLIF(COUNT(*),0) AS CONVERSION_RATE '
         ||     'FROM STG_CAMPAIGN_EVENTS '
         ||     'WHERE EVENT_DATE >= :score_dt - 90 '
         ||     'GROUP BY CUSTOMER_ID '
         || ') camp ON camp.CUSTOMER_ID = d.CUSTOMER_ID '
         || 'LEFT JOIN ( '
         ||     'SELECT CUSTOMER_ID, '
         ||            'ROUND(COUNT(*) / 30.0, 4) AS INTERACT_SCORE '
         ||     'FROM STG_CUSTOMER_INTERACTIONS '
         ||     'WHERE INTERACTION_DATE >= :score_dt - 30 '
         ||     'GROUP BY CUSTOMER_ID '
         || ') inter ON inter.CUSTOMER_ID = d.CUSTOMER_ID '
         || 'WHERE d.IS_CURRENT     = ''Y'' '
         || 'AND   d.CUSTOMER_SEGMENT = :seg';

        EXECUTE IMMEDIATE v_sql
            USING p_score_date, p_model_version, p_batch_id,
                  p_score_date, p_score_date, p_segment_code;

        v_rows := SQL%ROWCOUNT;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('<<< LOAD_FACT_CUSTOMER_SCORES: ' || v_rows || ' scores inserted');
    END LOAD_FACT_CUSTOMER_SCORES;

    -- -------------------------------------------------------------------------
    -- PROC: AGGREGATE_CAMPAIGN_PERFORMANCE
    -- -------------------------------------------------------------------------
    PROCEDURE AGGREGATE_CAMPAIGN_PERFORMANCE(
        p_run_date    IN DATE,
        p_campaign_id IN VARCHAR2 DEFAULT 'ALL'
    ) IS
        v_camp_filter VARCHAR2(100);
    BEGIN
        DBMS_OUTPUT.PUT_LINE('>>> AGGREGATE_CAMPAIGN_PERFORMANCE: ' || p_campaign_id);

        v_camp_filter := CASE UPPER(p_campaign_id)
            WHEN 'ALL' THEN '1=1'
            ELSE 'e.CAMPAIGN_ID = ''' || p_campaign_id || ''''
        END;

        EXECUTE IMMEDIATE
            'INSERT INTO FACT_CAMPAIGN_PERFORMANCE ( '
         || '    PERF_KEY, CAMPAIGN_ID, PERFORMANCE_DATE, '
         || '    TOTAL_SENT, TOTAL_OPENED, TOTAL_CLICKED, '
         || '    TOTAL_CONVERTED, TOTAL_UNSUBSCRIBED, REVENUE_ATTRIBUTED, '
         || '    OPEN_RATE, CLICK_RATE, CONVERSION_RATE, LOAD_DATE '
         || ') '
         || 'SELECT SEQ_FACT_CAMP_PERF.NEXTVAL, '
         || '       e.CAMPAIGN_ID, '
         || '       TRUNC(e.EVENT_DATE), '
         || '       SUM(CASE WHEN e.EVENT_TYPE = ''SENT''         THEN 1 ELSE 0 END), '
         || '       SUM(CASE WHEN e.EVENT_TYPE = ''OPENED''       THEN 1 ELSE 0 END), '
         || '       SUM(CASE WHEN e.EVENT_TYPE = ''CLICKED''      THEN 1 ELSE 0 END), '
         || '       SUM(CASE WHEN e.EVENT_TYPE = ''CONVERTED''    THEN 1 ELSE 0 END), '
         || '       SUM(CASE WHEN e.EVENT_TYPE = ''UNSUBSCRIBED'' THEN 1 ELSE 0 END), '
         || '       SUM(e.REVENUE_ATTRIBUTED), '
         || '       ROUND(SUM(CASE WHEN e.EVENT_TYPE=''OPENED'' THEN 1 ELSE 0 END) '
         ||              '/ NULLIF(SUM(CASE WHEN e.EVENT_TYPE=''SENT'' THEN 1 ELSE 0 END),0), 6), '
         || '       ROUND(SUM(CASE WHEN e.EVENT_TYPE=''CLICKED'' THEN 1 ELSE 0 END) '
         ||              '/ NULLIF(SUM(CASE WHEN e.EVENT_TYPE=''OPENED'' THEN 1 ELSE 0 END),0), 6), '
         || '       ROUND(SUM(CASE WHEN e.EVENT_TYPE=''CONVERTED'' THEN 1 ELSE 0 END) '
         ||              '/ NULLIF(SUM(CASE WHEN e.EVENT_TYPE=''SENT'' THEN 1 ELSE 0 END),0), 6), '
         || '       SYSDATE '
         || 'FROM STG_CAMPAIGN_EVENTS e '
         || 'WHERE TRUNC(e.EVENT_DATE) = :run_dt '
         || 'AND   ' || v_camp_filter
         || ' GROUP BY e.CAMPAIGN_ID, TRUNC(e.EVENT_DATE)'
        USING p_run_date;

        COMMIT;
        DBMS_OUTPUT.PUT_LINE('<<< AGGREGATE_CAMPAIGN_PERFORMANCE: ' || SQL%ROWCOUNT || ' rows');
    END AGGREGATE_CAMPAIGN_PERFORMANCE;

    -- -------------------------------------------------------------------------
    -- PROC: GENERATE_SEGMENT_SUMMARY
    -- Reads from FACT_CUSTOMER_SCORES + DW_OWNER.FACT_REGIONAL_SUMMARY (cross-repo)
    -- -------------------------------------------------------------------------
    PROCEDURE GENERATE_SEGMENT_SUMMARY(
        p_summary_date    IN DATE,
        p_region_code     IN VARCHAR2 DEFAULT 'ALL'
    ) IS
        v_reg_filter VARCHAR2(100);
    BEGIN
        DBMS_OUTPUT.PUT_LINE('>>> GENERATE_SEGMENT_SUMMARY: ' || TO_CHAR(p_summary_date,'YYYY-MM-DD'));

        v_reg_filter := CASE UPPER(p_region_code)
            WHEN 'ALL' THEN '1=1'
            ELSE 'sc.REGION_CODE = ''' || UPPER(p_region_code) || ''''
        END;

        DELETE FROM FACT_CUSTOMER_SEGMENT_SUMMARY
        WHERE SUMMARY_DATE = p_summary_date
        AND   (p_region_code = 'ALL' OR REGION_CODE = p_region_code);

        INSERT INTO FACT_CUSTOMER_SEGMENT_SUMMARY (
            SEGMENT_SUMMARY_KEY, SEGMENT_CODE, REGION_CODE, SUMMARY_DATE,
            CUSTOMER_COUNT, AVG_CLV_SCORE, AVG_CHURN_RISK,
            HIGH_VALUE_COUNT, AT_RISK_COUNT,
            TOTAL_RETAIL_SPEND,
            LOAD_DATE
        )
        SELECT SEQ_SEGMENT_SUMMARY.NEXTVAL,
               sc.SEGMENT_CODE,
               sc.REGION_CODE,
               p_summary_date,
               COUNT(DISTINCT sc.CUSTOMER_ID)                           AS CUSTOMER_COUNT,
               ROUND(AVG(sc.CLV_SCORE), 4)                              AS AVG_CLV_SCORE,
               ROUND(AVG(sc.CHURN_RISK_SCORE), 6)                       AS AVG_CHURN_RISK,
               COUNT(CASE WHEN sc.CLV_SCORE > 5 THEN 1 END)             AS HIGH_VALUE_COUNT,
               COUNT(CASE WHEN sc.CHURN_RISK_SCORE > 0.7 THEN 1 END)    AS AT_RISK_COUNT,
               -- Pull retail spend from cross-repo FACT_REGIONAL_SUMMARY (DW_OWNER)
               NVL(SUM(rs.TOTAL_REVENUE), 0)                            AS TOTAL_RETAIL_SPEND,
               SYSDATE
        FROM   FACT_CUSTOMER_SCORES sc
        JOIN   DIM_CUSTOMER_CRM dc
               ON dc.CUSTOMER_ID  = sc.CUSTOMER_ID AND dc.IS_CURRENT = 'Y'
        LEFT JOIN DW_OWNER.FACT_REGIONAL_SUMMARY rs    -- cross-repo
               ON rs.REGION_CODE  = dc.REGION_CODE
              AND rs.SUMMARY_DATE = p_summary_date
        WHERE  sc.SCORE_DATE = p_summary_date
        AND    ' || v_reg_filter
        GROUP  BY sc.SEGMENT_CODE, sc.REGION_CODE;

        COMMIT;
        DBMS_OUTPUT.PUT_LINE('<<< GENERATE_SEGMENT_SUMMARY: ' || SQL%ROWCOUNT || ' rows');
    END GENERATE_SEGMENT_SUMMARY;

    -- -------------------------------------------------------------------------
    -- PROC: MASTER_CRM_LOAD
    -- -------------------------------------------------------------------------
    PROCEDURE MASTER_CRM_LOAD(
        p_run_date        IN DATE,
        p_segment_code    IN VARCHAR2,
        p_region_code     IN VARCHAR2
    ) IS
        v_batch_id NUMBER;
        v_rows     NUMBER := 0;
    BEGIN
        v_batch_id := SEQ_CRM_BATCH_ID.NEXTVAL;
        DBMS_OUTPUT.PUT_LINE('=== MASTER_CRM_LOAD: batch=' || v_batch_id
            || ' date=' || TO_CHAR(p_run_date,'YYYY-MM-DD')
            || ' segment=' || p_segment_code || ' region=' || p_region_code);

        LOAD_DIM_CUSTOMER_CRM(p_run_date, p_segment_code, p_region_code, v_rows);
        LOAD_FACT_CUSTOMER_SCORES(p_run_date, p_segment_code, 'v2.1', v_batch_id);
        AGGREGATE_CAMPAIGN_PERFORMANCE(p_run_date, 'ALL');
        GENERATE_SEGMENT_SUMMARY(p_run_date, p_region_code);

        DBMS_OUTPUT.PUT_LINE('=== MASTER_CRM_LOAD COMPLETE: rows=' || v_rows);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('FATAL in MASTER_CRM_LOAD: ' || SQLERRM);
            RAISE;
    END MASTER_CRM_LOAD;

END PKG_CUSTOMER_HISTORIZATION;
/
