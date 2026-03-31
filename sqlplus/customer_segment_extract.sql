-- =============================================================================
-- SQL*Plus Script : customer_segment_extract.sql
-- Purpose        : Extracts customer profiles from source CRM system into
--                  STG_CUSTOMER_PROFILE, filtered by segment and run date.
--                  Also pulls campaign events and interaction data.
--                  Uses &&-style substitution variables from shell.
--
-- Called by      : process_customer_data.ksh
-- Variables set by caller:
--   &&RUN_DATE          - e.g. 2024-01-15
--   &&CUSTOMER_SEGMENT  - e.g. VIP  (or ALL)
--   &&BATCH_SIZE        - e.g. 5000
--   &&REGION_CODE       - e.g. NORTH (or ALL)
--   &&RUN_DATE_FMT      - e.g. 20240115 (for file naming)
-- =============================================================================

SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 300
SET TRIMSPOOL ON

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK;
WHENEVER OSERROR  EXIT 9 ROLLBACK;

SPOOL /opt/etl/logs/crm/crm_extract_&&CUSTOMER_SEGMENT._&&RUN_DATE_FMT..log APPEND

PROMPT ============================================================
PROMPT customer_segment_extract.sql
PROMPT RUN_DATE         = &&RUN_DATE
PROMPT CUSTOMER_SEGMENT = &&CUSTOMER_SEGMENT
PROMPT BATCH_SIZE       = &&BATCH_SIZE
PROMPT REGION_CODE      = &&REGION_CODE
PROMPT ============================================================

-- ----------------------------------------------------------------------------
-- Step 1: Clear PENDING records for this date/segment (idempotent)
-- ----------------------------------------------------------------------------
PROMPT Step 1: Clearing prior PENDING records...

DELETE FROM STG_CUSTOMER_PROFILE
WHERE  LOAD_DATE        = TO_DATE('&&RUN_DATE', 'YYYY-MM-DD')
AND    ETL_STATUS       = 'PENDING'
AND    (CUSTOMER_SEGMENT = '&&CUSTOMER_SEGMENT' OR '&&CUSTOMER_SEGMENT' = 'ALL');

PROMPT Deleted &&_ROWCOUNT prior PENDING records.

-- ----------------------------------------------------------------------------
-- Step 2: Extract customer profiles from SOURCE_CRM
--         Applies BATCH_SIZE limit via ROWNUM for memory management
--         CUSTOMER_SEGMENT filter is dynamic: if ALL, no filter applied
-- ----------------------------------------------------------------------------
PROMPT Step 2: Extracting customer profiles (segment=&&CUSTOMER_SEGMENT, batch=&&BATCH_SIZE)...

INSERT /*+ APPEND */ INTO STG_CUSTOMER_PROFILE (
    CUSTOMER_ID, CUSTOMER_CODE, FIRST_NAME, LAST_NAME,
    DATE_OF_BIRTH, EMAIL, PHONE, MOBILE,
    ADDRESS_LINE1, CITY, POSTCODE, COUNTRY_CODE,
    REGION_CODE, CUSTOMER_SEGMENT, ACQUISITION_CHANNEL,
    REGISTRATION_DATE,
    IS_OPTED_IN_EMAIL, IS_OPTED_IN_SMS,
    LOAD_DATE, ETL_STATUS
)
SELECT *
FROM (
    SELECT
        c.CUST_ID                                       AS CUSTOMER_ID,
        c.CUST_REF                                      AS CUSTOMER_CODE,
        c.GIVEN_NAME                                    AS FIRST_NAME,
        c.FAMILY_NAME                                   AS LAST_NAME,
        c.DOB                                           AS DATE_OF_BIRTH,
        LOWER(c.EMAIL)                                  AS EMAIL,
        c.TEL_HOME                                      AS PHONE,
        c.TEL_MOBILE                                    AS MOBILE,
        c.ADDR_LINE1                                    AS ADDRESS_LINE1,
        c.ADDR_CITY                                     AS CITY,
        c.ADDR_POSTCODE                                 AS POSTCODE,
        NVL(c.COUNTRY_ISO, 'GB')                        AS COUNTRY_CODE,
        c.REGION_CODE,
        NVL(seg.SEGMENT_CODE, 'RETAIL')                 AS CUSTOMER_SEGMENT,
        NVL(c.ACQ_CHANNEL_CODE, 'UNKNOWN')              AS ACQUISITION_CHANNEL,
        c.CREATED_DATE                                  AS REGISTRATION_DATE,
        NVL(c.EMAIL_OPT_IN, 'N')                        AS IS_OPTED_IN_EMAIL,
        NVL(c.SMS_OPT_IN,   'N')                        AS IS_OPTED_IN_SMS,
        TO_DATE('&&RUN_DATE', 'YYYY-MM-DD')             AS LOAD_DATE,
        'PENDING'                                       AS ETL_STATUS
    FROM   SOURCE_CRM.CUSTOMERS c
    LEFT JOIN SOURCE_CRM.CUSTOMER_SEGMENTS seg
           ON seg.CUST_ID = c.CUST_ID
          AND seg.IS_PRIMARY = 'Y'
          AND seg.VALID_TO  >= TO_DATE('&&RUN_DATE','YYYY-MM-DD')
    WHERE  c.IS_ACTIVE = 'Y'
    AND    c.LAST_MODIFIED_DATE >= TO_DATE('&&RUN_DATE','YYYY-MM-DD') - 7
    AND    (
               '&&CUSTOMER_SEGMENT' = 'ALL'
            OR NVL(seg.SEGMENT_CODE, 'RETAIL') = '&&CUSTOMER_SEGMENT'
           )
    AND    (
               '&&REGION_CODE' = 'ALL'
            OR c.REGION_CODE = '&&REGION_CODE'
           )
    ORDER BY c.LAST_MODIFIED_DATE DESC
)
WHERE ROWNUM <= &&BATCH_SIZE;

COMMIT;
PROMPT Inserted &&_ROWCOUNT customer profiles.

-- ----------------------------------------------------------------------------
-- Step 3: Extract campaign events for the same customer set
-- ----------------------------------------------------------------------------
PROMPT Step 3: Extracting campaign events for past 90 days...

DELETE FROM STG_CAMPAIGN_EVENTS
WHERE LOAD_DATE = TO_DATE('&&RUN_DATE','YYYY-MM-DD');

INSERT /*+ APPEND */ INTO STG_CAMPAIGN_EVENTS (
    EVENT_ID, CUSTOMER_ID, CAMPAIGN_ID, CAMPAIGN_NAME,
    EVENT_TYPE, EVENT_DATE, CHANNEL, OFFER_CODE,
    REVENUE_ATTRIBUTED, LOAD_DATE
)
SELECT
    e.EVENT_ID,
    e.CUST_ID                                   AS CUSTOMER_ID,
    e.CAMP_REF                                  AS CAMPAIGN_ID,
    c.CAMP_NAME                                 AS CAMPAIGN_NAME,
    e.EVENT_TYPE_CD                             AS EVENT_TYPE,
    e.EVENT_DTTM                                AS EVENT_DATE,
    e.CHANNEL_CD                                AS CHANNEL,
    e.OFFER_CODE,
    NVL(e.REVENUE_ATTRIB, 0)                    AS REVENUE_ATTRIBUTED,
    TO_DATE('&&RUN_DATE','YYYY-MM-DD')          AS LOAD_DATE
FROM   SOURCE_CRM.CAMPAIGN_EVENTS e
JOIN   SOURCE_CRM.CAMPAIGNS c ON c.CAMP_REF = e.CAMP_REF
WHERE  e.EVENT_DTTM >= TO_DATE('&&RUN_DATE','YYYY-MM-DD') - 90
AND    e.EVENT_DTTM <  TO_DATE('&&RUN_DATE','YYYY-MM-DD') + 1
AND    EXISTS (
           SELECT 1 FROM STG_CUSTOMER_PROFILE sp
           WHERE  sp.CUSTOMER_ID = e.CUST_ID
           AND    sp.LOAD_DATE   = TO_DATE('&&RUN_DATE','YYYY-MM-DD')
       );

COMMIT;
PROMPT Campaign events inserted: &&_ROWCOUNT rows.

-- ----------------------------------------------------------------------------
-- Step 4: Extract recent customer interactions (30 days)
-- ----------------------------------------------------------------------------
PROMPT Step 4: Extracting customer interactions (30 days)...

DELETE FROM STG_CUSTOMER_INTERACTIONS
WHERE LOAD_DATE = TO_DATE('&&RUN_DATE','YYYY-MM-DD');

INSERT /*+ APPEND */ INTO STG_CUSTOMER_INTERACTIONS (
    INTERACTION_ID, CUSTOMER_ID, INTERACTION_TYPE,
    INTERACTION_DATE, CHANNEL, AGENT_ID,
    RESOLUTION_CODE, SATISFACTION_SCORE, DURATION_MINUTES,
    LOAD_DATE
)
SELECT
    i.INTERACTION_ID,
    i.CUST_ID,
    i.INTERACTION_TYPE_CD,
    i.INTERACTION_DATE,
    i.CHANNEL_CD,
    i.AGENT_ID,
    i.RESOLUTION_CD,
    i.CSAT_SCORE,
    i.DURATION_MINS,
    TO_DATE('&&RUN_DATE','YYYY-MM-DD')
FROM   SOURCE_CRM.CUSTOMER_INTERACTIONS i
WHERE  i.INTERACTION_DATE >= TO_DATE('&&RUN_DATE','YYYY-MM-DD') - 30
AND    i.INTERACTION_DATE <  TO_DATE('&&RUN_DATE','YYYY-MM-DD') + 1
AND    EXISTS (
           SELECT 1 FROM STG_CUSTOMER_PROFILE sp
           WHERE  sp.CUSTOMER_ID = i.CUST_ID
           AND    sp.LOAD_DATE   = TO_DATE('&&RUN_DATE','YYYY-MM-DD')
       );

COMMIT;
PROMPT Interactions inserted: &&_ROWCOUNT rows.

-- ----------------------------------------------------------------------------
-- Step 5: Summary report
-- ----------------------------------------------------------------------------
PROMPT Step 5: Summary for &&CUSTOMER_SEGMENT / &&RUN_DATE...

SELECT '&&CUSTOMER_SEGMENT'   AS segment,
       COUNT(*)               AS total_customers,
       SUM(CASE WHEN IS_OPTED_IN_EMAIL='Y' THEN 1 ELSE 0 END) AS email_opted_in,
       COUNT(DISTINCT REGION_CODE)                             AS regions
FROM STG_CUSTOMER_PROFILE
WHERE LOAD_DATE       = TO_DATE('&&RUN_DATE','YYYY-MM-DD')
AND   ETL_STATUS      = 'PENDING';

SPOOL OFF
PROMPT customer_segment_extract.sql DONE.
EXIT 0;
