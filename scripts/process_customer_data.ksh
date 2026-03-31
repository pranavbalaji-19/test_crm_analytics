#!/bin/ksh
# =============================================================================
# Script  : process_customer_data.ksh
# Purpose : CRM customer data processing pipeline. Accepts CUSTOMER_SEGMENT,
#           RUN_DATE, and BATCH_SIZE as positional params. Resolves variables
#           into SQL*Plus substitution vars. Sources retry_handler.ksh for
#           retry/audit logic. Calls SQL*Plus, then PL/SQL, then Python scorer.
#
# Usage   : process_customer_data.ksh <RUN_DATE> <CUSTOMER_SEGMENT> <BATCH_SIZE>
# Example : process_customer_data.ksh 2024-01-15 VIP 5000
#
# Called by: UC4 job CRM_CUSTOMER_SEGMENT_LOAD
# Prerequisites (cross-repo):
#   - FINANCE_DAILY_WORKFLOW / FINANCE_DAILY_GL_CLOSE (event: FINANCE_GL_CLOSE_COMPLETE)
#   - RETAIL_DAILY_WORKFLOW  / RETAIL_COMPLETION_NOTIFY (event: RETAIL_DAILY_COMPLETE)
# =============================================================================

set -u

# ----------------------------------------------------------------------------
# 1. Parameter resolution (REQ-PRE-01)
# ----------------------------------------------------------------------------
RUN_DATE=${1:?"RUN_DATE (YYYY-MM-DD) required"}
CUSTOMER_SEGMENT=${2:?"CUSTOMER_SEGMENT required (VIP|RETAIL|WHOLESALE|ALL)"}
BATCH_SIZE=${3:-5000}

echo "$RUN_DATE" | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' || {
    echo "ERROR: Invalid RUN_DATE format: $RUN_DATE"
    exit 1
}

RUN_DATE_FMT=$(echo $RUN_DATE | tr '-' '')

# Validate segment
case "$CUSTOMER_SEGMENT" in
    VIP|RETAIL|WHOLESALE|ALL) ;;
    *) echo "ERROR: Unknown CUSTOMER_SEGMENT: $CUSTOMER_SEGMENT"; exit 1 ;;
esac

# ----------------------------------------------------------------------------
# 2. Environment setup
# ----------------------------------------------------------------------------
ENV_CONFIG="${ENV_CONFIG_DIR:-/opt/etl/config}/env_crm.properties"
. "$ENV_CONFIG"

# Source shared retry handler library
. "${ETL_LIB_DIR:-/opt/etl/scripts}/retry_handler.ksh"

LOG_DIR="${LOG_DIR:-/opt/etl/logs/crm}"
LOG_FILE="${LOG_DIR}/crm_process_${CUSTOMER_SEGMENT}_${RUN_DATE_FMT}_$(date '+%H%M%S').log"
SQLPLUS_DIR="${SQLPLUS_DIR:-/opt/etl/sqlplus}"
ORA_CONNECT="${DB_USER}/${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_SID}"

# Derived variables
REGION_CODE="${REGION_CODE:-ALL}"
MODEL_VERSION="${SCORE_MODEL_VERSION:-v2.1}"

# Segment-specific batch size override
case "$CUSTOMER_SEGMENT" in
    VIP)       BATCH_SIZE=${VIP_BATCH_SIZE:-${BATCH_SIZE}} ;;
    RETAIL)    BATCH_SIZE=${RETAIL_BATCH_SIZE:-${BATCH_SIZE}} ;;
    WHOLESALE) BATCH_SIZE=${WHOLESALE_BATCH_SIZE:-${BATCH_SIZE}} ;;
esac

export RUN_DATE CUSTOMER_SEGMENT BATCH_SIZE RUN_DATE_FMT REGION_CODE MODEL_VERSION

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${CUSTOMER_SEGMENT}] $1" | tee -a "$LOG_FILE"; }

log "============================================================"
log "process_customer_data.ksh STARTED"
log "RUN_DATE=$RUN_DATE  CUSTOMER_SEGMENT=$CUSTOMER_SEGMENT"
log "BATCH_SIZE=$BATCH_SIZE  REGION_CODE=$REGION_CODE  MODEL_VERSION=$MODEL_VERSION"
log "============================================================"

log_job_audit "CRM_WEEKLY_WORKFLOW" "CRM_CUSTOMER_SEGMENT_LOAD" "$RUN_DATE" "RUNNING" 0

# ----------------------------------------------------------------------------
# 3. Cross-repo prerequisite checks
# ----------------------------------------------------------------------------
log "Step 0: Checking cross-repo prerequisites..."

# Check Finance GL close completed (finance_etl_legacy)
wait_for_event "FINANCE_GL_CLOSE_COMPLETE" "$RUN_DATE" 60 30
if [ $? -ne 0 ]; then
    log "ERROR: Finance GL close not completed within timeout. Cannot load customer financial scores."
    exit 2
fi
log "  FINANCE_GL_CLOSE_COMPLETE: OK"

# Check Retail daily load completed (retail_dw_legacy)
wait_for_event "RETAIL_DAILY_COMPLETE" "$RUN_DATE" 30 30
if [ $? -ne 0 ]; then
    log "WARN: RETAIL_DAILY_COMPLETE event not received. Customer retail spend data may be incomplete."
    # Non-fatal: continue with available data
fi
log "  RETAIL_DAILY_COMPLETE: OK (or continuing without)"

# ----------------------------------------------------------------------------
# 4. Step 1: SQL*Plus - Extract and stage customer profiles
#    Passes CUSTOMER_SEGMENT, RUN_DATE, BATCH_SIZE as substitution vars
# ----------------------------------------------------------------------------
log "Step 1: Extracting customer profiles via SQL*Plus..."

SQLPLUS_CMD="sqlplus -s '$ORA_CONNECT' <<EOF
    DEFINE RUN_DATE          = '$RUN_DATE'
    DEFINE CUSTOMER_SEGMENT  = '$CUSTOMER_SEGMENT'
    DEFINE BATCH_SIZE        = '$BATCH_SIZE'
    DEFINE REGION_CODE       = '$REGION_CODE'
    DEFINE RUN_DATE_FMT      = '$RUN_DATE_FMT'

    @${SQLPLUS_DIR}/customer_segment_extract.sql
    EXIT SQL.SQLCODE;
EOF"

retry_command "$SQLPLUS_CMD" "${MAX_RETRIES:-3}" 60 "exponential"
if [ $? -ne 0 ]; then
    log "ERROR: SQL*Plus extraction failed after retries"
    log_job_audit "CRM_WEEKLY_WORKFLOW" "CRM_CUSTOMER_SEGMENT_LOAD" "$RUN_DATE" "FAILED" 0
    exit 3
fi
log "Step 1: SQL*Plus extraction complete."

# ----------------------------------------------------------------------------
# 5. Step 2: Validate staging counts
# ----------------------------------------------------------------------------
log "Step 2: Validating staging counts..."

STG_COUNT=$(sqlplus -s "$ORA_CONNECT" <<COUNT_EOF
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
    SELECT COUNT(*) FROM STG_CUSTOMER_PROFILE
    WHERE  LOAD_DATE         = TO_DATE('$RUN_DATE','YYYY-MM-DD')
    AND    CUSTOMER_SEGMENT   = '$CUSTOMER_SEGMENT'
    AND    ETL_STATUS         = 'PENDING';
    EXIT;
COUNT_EOF
)
STG_COUNT=$(echo $STG_COUNT | tr -d ' ')
log "Staging rows (PENDING): $STG_COUNT"

if [ -z "$STG_COUNT" ] || [ "$STG_COUNT" -eq 0 ]; then
    log "WARN: No staging records for $CUSTOMER_SEGMENT on $RUN_DATE"
    if [ "${ALLOW_EMPTY_SEGMENT:-N}" = "Y" ]; then
        log "ALLOW_EMPTY_SEGMENT=Y: exiting gracefully."
        log_job_audit "CRM_WEEKLY_WORKFLOW" "CRM_CUSTOMER_SEGMENT_LOAD" "$RUN_DATE" "NO_DATA" 0
        exit 0
    fi
fi

# ----------------------------------------------------------------------------
# 6. Step 3: PL/SQL MASTER_CRM_LOAD
# ----------------------------------------------------------------------------
log "Step 3: Running MASTER_CRM_LOAD for segment=$CUSTOMER_SEGMENT..."

PLSQL_CMD="sqlplus -s '$ORA_CONNECT' <<PLSQL_EOF
    SET SERVEROUTPUT ON SIZE UNLIMITED FEEDBACK OFF
    BEGIN
        PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD(
            p_run_date     => TO_DATE('$RUN_DATE','YYYY-MM-DD'),
            p_segment_code => '$CUSTOMER_SEGMENT',
            p_region_code  => '$REGION_CODE'
        );
    END;
    /
    EXIT SQL.SQLCODE;
PLSQL_EOF"

retry_command "$PLSQL_CMD" "${MAX_RETRIES:-3}" 90 "linear"
if [ $? -ne 0 ]; then
    log "ERROR: PL/SQL MASTER_CRM_LOAD failed after retries"
    log_job_audit "CRM_WEEKLY_WORKFLOW" "CRM_CUSTOMER_SEGMENT_LOAD" "$RUN_DATE" "FAILED" 0
    exit 4
fi
log "Step 3: MASTER_CRM_LOAD complete."

# ----------------------------------------------------------------------------
# 7. Step 4: Python customer scoring
# ----------------------------------------------------------------------------
log "Step 4: Running Python customer scoring model..."

PYTHON_CMD="python3 ${PYTHON_DIR:-/opt/etl/python}/customer_scoring.py \
    --run-date '$RUN_DATE' \
    --segment '$CUSTOMER_SEGMENT' \
    --batch-size $BATCH_SIZE \
    --model-version '$MODEL_VERSION' \
    --region '$REGION_CODE'"

retry_command "$PYTHON_CMD" 2 120 "fixed"
if [ $? -ne 0 ]; then
    log "WARN: Python scoring failed - scores may be incomplete (non-fatal)"
fi

# ----------------------------------------------------------------------------
# 8. Post-load summary
# ----------------------------------------------------------------------------
SCORE_COUNT=$(sqlplus -s "$ORA_CONNECT" <<SCORE_EOF
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
    SELECT COUNT(*) FROM FACT_CUSTOMER_SCORES
    WHERE  SCORE_DATE     = TO_DATE('$RUN_DATE','YYYY-MM-DD')
    AND    SEGMENT_CODE   = '$CUSTOMER_SEGMENT';
    EXIT;
SCORE_EOF
)
SCORE_COUNT=$(echo $SCORE_COUNT | tr -d ' ')
log "FACT_CUSTOMER_SCORES rows loaded: $SCORE_COUNT"

log_job_audit "CRM_WEEKLY_WORKFLOW" "CRM_CUSTOMER_SEGMENT_LOAD" "$RUN_DATE" "SUCCESS" "$SCORE_COUNT"

log "============================================================"
log "process_customer_data.ksh COMPLETED"
log "SEGMENT=$CUSTOMER_SEGMENT RUN_DATE=$RUN_DATE SCORES=$SCORE_COUNT"
log "============================================================"

exit 0
