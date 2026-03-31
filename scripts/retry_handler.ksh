#!/bin/ksh
# =============================================================================
# Script  : retry_handler.ksh
# Purpose : Shared retry handler library sourced by CRM ETL scripts.
#           Provides: retry_command(), wait_for_event(), check_prereq_job()
#           functions with configurable backoff strategies.
#
# Usage   : . /opt/etl/scripts/retry_handler.ksh
#           retry_command "sqlplus ..." 3 60 "exponential"
# =============================================================================

# Global retry state
RETRY_LOG_FILE="${LOG_FILE:-/tmp/retry_handler.log}"

# ----------------------------------------------------------------------------
# Function: retry_command
# Args: $1=command_string, $2=max_retries, $3=base_wait_sec, $4=backoff_type
# Backoff types: linear | exponential | fixed
# Returns: 0 on success, 1 on exhausted retries
# ----------------------------------------------------------------------------
retry_command() {
    CMD="$1"
    MAX_TRIES=${2:-3}
    BASE_WAIT=${3:-60}
    BACKOFF=${4:-"exponential"}

    ATTEMPT=0
    while [ $ATTEMPT -lt $MAX_TRIES ]; do
        ATTEMPT=$((ATTEMPT + 1))
        echo "[$(date '+%H:%M:%S')] [RETRY] Attempt $ATTEMPT/$MAX_TRIES: $CMD" | tee -a "$RETRY_LOG_FILE"

        eval "$CMD"
        CMD_RC=$?

        if [ $CMD_RC -eq 0 ]; then
            echo "[$(date '+%H:%M:%S')] [RETRY] SUCCESS on attempt $ATTEMPT" | tee -a "$RETRY_LOG_FILE"
            return 0
        fi

        echo "[$(date '+%H:%M:%S')] [RETRY] FAILED (rc=$CMD_RC) attempt $ATTEMPT" | tee -a "$RETRY_LOG_FILE"

        if [ $ATTEMPT -lt $MAX_TRIES ]; then
            case "$BACKOFF" in
                exponential) WAIT=$((BASE_WAIT * ATTEMPT * ATTEMPT)) ;;
                linear)      WAIT=$((BASE_WAIT * ATTEMPT))           ;;
                fixed)       WAIT=$BASE_WAIT                          ;;
                *)           WAIT=$BASE_WAIT                          ;;
            esac
            echo "[$(date '+%H:%M:%S')] [RETRY] Waiting ${WAIT}s (backoff=$BACKOFF)..." | tee -a "$RETRY_LOG_FILE"
            sleep $WAIT
        fi
    done

    echo "[$(date '+%H:%M:%S')] [RETRY] EXHAUSTED after $MAX_TRIES attempts" | tee -a "$RETRY_LOG_FILE"
    return 1
}

# ----------------------------------------------------------------------------
# Function: wait_for_event
# Polls for a UC4 published event to appear (cross-workflow dependency check).
# Args: $1=event_name, $2=event_value, $3=timeout_minutes, $4=poll_interval_sec
# ----------------------------------------------------------------------------
wait_for_event() {
    EVENT_NAME="$1"
    EVENT_VALUE="$2"
    TIMEOUT_MIN=${3:-60}
    POLL_SEC=${4:-30}

    ELAPSED=0
    MAX_ELAPSED=$((TIMEOUT_MIN * 60))

    echo "[$(date '+%H:%M:%S')] [WAIT_EVENT] Waiting for event: $EVENT_NAME=$EVENT_VALUE (timeout=${TIMEOUT_MIN}m)"

    while [ $ELAPSED -lt $MAX_ELAPSED ]; do
        # Query UC4 event API
        EVENT_STATUS=$(uc4api check_event "$EVENT_NAME" "$EVENT_VALUE" 2>/dev/null)
        if [ "$EVENT_STATUS" = "PUBLISHED" ]; then
            echo "[$(date '+%H:%M:%S')] [WAIT_EVENT] Event received: $EVENT_NAME=$EVENT_VALUE"
            return 0
        fi

        # Fallback: check marker file left by upstream workflow
        MARKER_FILE="/opt/etl/events/${EVENT_NAME}_${EVENT_VALUE}.done"
        if [ -f "$MARKER_FILE" ]; then
            echo "[$(date '+%H:%M:%S')] [WAIT_EVENT] Marker file found: $MARKER_FILE"
            return 0
        fi

        sleep $POLL_SEC
        ELAPSED=$((ELAPSED + POLL_SEC))
        echo "[$(date '+%H:%M:%S')] [WAIT_EVENT] Still waiting... elapsed=${ELAPSED}s/${MAX_ELAPSED}s"
    done

    echo "[$(date '+%H:%M:%S')] [WAIT_EVENT] TIMEOUT waiting for $EVENT_NAME after ${TIMEOUT_MIN}m"
    return 2
}

# ----------------------------------------------------------------------------
# Function: check_prereq_job
# Verifies a cross-repo predecessor job completed successfully.
# Checks Oracle audit table written by upstream workflow.
# Args: $1=workflow_name, $2=job_name, $3=run_date
# ----------------------------------------------------------------------------
check_prereq_job() {
    PREREQ_WORKFLOW="$1"
    PREREQ_JOB="$2"
    CHECK_DATE="$3"
    ORA_CONNECT="${4:-$ORA_CONNECT}"

    PREREQ_STATUS=$(sqlplus -s "$ORA_CONNECT" <<PREREQ_EOF
        SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
        SELECT NVL(MAX(STATUS),'NOT_FOUND')
        FROM   ETL_JOB_AUDIT
        WHERE  WORKFLOW_NAME = '$PREREQ_WORKFLOW'
        AND    JOB_NAME      = '$PREREQ_JOB'
        AND    RUN_DATE      = TO_DATE('$CHECK_DATE','YYYY-MM-DD');
        EXIT;
PREREQ_EOF
    )

    PREREQ_STATUS=$(echo $PREREQ_STATUS | tr -d ' ')
    echo "[CHECK_PREREQ] $PREREQ_WORKFLOW/$PREREQ_JOB on $CHECK_DATE: STATUS=$PREREQ_STATUS"

    case "$PREREQ_STATUS" in
        SUCCESS|COMPLETED)  return 0 ;;
        RUNNING)            return 2 ;;   # still running
        NOT_FOUND)          return 3 ;;   # not started
        *)                  return 1 ;;   # failed or unknown
    esac
}

# ----------------------------------------------------------------------------
# Function: log_job_audit
# Writes job start/end status to ETL_JOB_AUDIT table.
# Args: $1=workflow, $2=job, $3=run_date, $4=status, $5=rows_processed
# ----------------------------------------------------------------------------
log_job_audit() {
    AUDIT_WF="$1"
    AUDIT_JOB="$2"
    AUDIT_DATE="$3"
    AUDIT_STATUS="$4"
    AUDIT_ROWS="${5:-0}"

    sqlplus -s "$ORA_CONNECT" <<AUDIT_EOF >> "${LOG_FILE:-/dev/null}" 2>&1
        MERGE INTO ETL_JOB_AUDIT tgt
        USING (SELECT '$AUDIT_WF' AS WF, '$AUDIT_JOB' AS JOB,
                      TO_DATE('$AUDIT_DATE','YYYY-MM-DD') AS RD FROM DUAL) src
        ON (tgt.WORKFLOW_NAME = src.WF AND tgt.JOB_NAME = src.JOB AND tgt.RUN_DATE = src.RD)
        WHEN MATCHED    THEN UPDATE SET tgt.STATUS = '$AUDIT_STATUS', tgt.ROWS_PROCESSED = $AUDIT_ROWS, tgt.UPDATED_DATE = SYSDATE
        WHEN NOT MATCHED THEN INSERT (WORKFLOW_NAME, JOB_NAME, RUN_DATE, STATUS, ROWS_PROCESSED, CREATED_DATE)
                              VALUES ('$AUDIT_WF', '$AUDIT_JOB', TO_DATE('$AUDIT_DATE','YYYY-MM-DD'), '$AUDIT_STATUS', $AUDIT_ROWS, SYSDATE);
        COMMIT;
        EXIT;
AUDIT_EOF
}
