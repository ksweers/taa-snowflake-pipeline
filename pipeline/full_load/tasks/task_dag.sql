USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE TASK TAA_FL_ROOT
    WAREHOUSE = WH_DS_AUTOMATION_TST
    SCHEDULE  = '11520 MINUTE'               -- 8 days: satisfies Snowflake RESUME requirement; task is always triggered manually via INGEST_TAA_LAUNCH_FULL_LOAD
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'TAA full load DAG root. Clears target tables and builds file manifest. Triggers all Wave 1 tasks upon success. Always triggered via INGEST_TAA_LAUNCH_FULL_LOAD -- schedule exists only to satisfy RESUME requirement.'
AS
CALL INGEST_TAA_FULL_LOAD_PREPARE();


-- =============================================================================
-- WAVE 1 TASKS  (8 tasks, all run in parallel immediately after ROOT)
-- =============================================================================


CREATE OR REPLACE TASK TAA_FL_W1_CUSTOMER
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 1: CUSTOMER (non-multi-tenant).'
    AFTER TAA_FL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('CUSTOMER');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FL_W1_ENTERPRISECUSTOMER
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 1: ENTERPRISECUSTOMER (non-multi-tenant).'
    AFTER TAA_FL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FL_W1_PAYTYPE
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 1: PAYTYPE.'
    AFTER TAA_FL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('PAYTYPE');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FL_W1_SCHEDULE
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 1: SCHEDULE.'
    AFTER TAA_FL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('SCHEDULE');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FL_W1_USERINFO
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 1: USERINFO. Prerequisite for TAA_FL_W2_USERINFOISSALARY.'
    AFTER TAA_FL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('USERINFO');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FL_W1_TIMEOFFDATA
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 1: TIMEOFFDATA.'
    AFTER TAA_FL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FL_W1_TIMEOFFREQUEST
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 1: TIMEOFFREQUEST. Prerequisite for TAA_FL_W2_TIMEOFFREQUESTDETAIL.'
    AFTER TAA_FL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST');

-- -----------------------------------------------------------------------------
-- Note: TIMESLICEPOST is the largest table (~98 columns). Size set to SMALL.


CREATE OR REPLACE TASK TAA_FL_W1_TIMESLICEPOST
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 1: TIMESLICEPOST (98-column table, sized SMALL). Prerequisite for Wave 2 detail tables.'
    AFTER TAA_FL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST');


-- =============================================================================
-- WAVE 2 TASKS  (4 tasks, each waits only for its specific Wave 1 prerequisite)
-- =============================================================================


CREATE OR REPLACE TASK TAA_FL_W2_USERINFOISSALARY
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 2: USERINFOISSALARY. Runs after USERINFO (USERID FK dependency).'
    AFTER TAA_FL_W1_USERINFO
AS
CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FL_W2_TIMEOFFREQUESTDETAIL
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 2: TIMEOFFREQUESTDETAIL. Runs after TIMEOFFREQUEST (FK dependency).'
    AFTER TAA_FL_W1_TIMEOFFREQUEST
AS
CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FL_W2_TIMESLICEPOSTEXCEPTIONDETAIL
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 2: TIMESLICEPOSTEXCEPTIONDETAIL. Runs after TIMESLICEPOST (FK dependency).'
    AFTER TAA_FL_W1_TIMESLICEPOST
AS
CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FL_W2_TIMESLICEPOSTSHIFTDIFFDETAIL
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load Wave 2: TIMESLICEPOSTSHIFTDIFFDETAIL. Runs after TIMESLICEPOST (FK dependency).'
    AFTER TAA_FL_W1_TIMESLICEPOST
AS
CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL');


-- =============================================================================
-- FINALIZE TASK
-- Runs after all 9 leaf tasks (5 Wave 1 leaves + 4 Wave 2 leaves).
-- =============================================================================


CREATE OR REPLACE TASK TAA_FL_FINALIZE
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA full load finalize: updates INGEST_TAA_FULL_LOAD_STATE after all 9 leaf tasks succeed.'
    AFTER
        TAA_FL_W1_CUSTOMER,
        TAA_FL_W1_ENTERPRISECUSTOMER,
        TAA_FL_W1_PAYTYPE,
        TAA_FL_W1_SCHEDULE,
        TAA_FL_W1_TIMEOFFDATA,
        TAA_FL_W2_USERINFOISSALARY,
        TAA_FL_W2_TIMEOFFREQUESTDETAIL,
        TAA_FL_W2_TIMESLICEPOSTEXCEPTIONDETAIL,
        TAA_FL_W2_TIMESLICEPOSTSHIFTDIFFDETAIL
AS
CALL INGEST_TAA_FULL_LOAD_FINALIZE();


-- =============================================================================
-- ENABLE THE TASK DAG
-- =============================================================================
-- Resume order: leaf tasks first, root task last.
-- Snowflake requires children to be RESUMED before the root can be RESUMED.
-- The root task only runs when triggered by INGEST_TAA_LAUNCH_FULL_LOAD.
-- =============================================================================



-- =============================================================================
-- QUICK REFERENCE
-- =============================================================================

-- TRIGGER A RUN (replaces CALL INGEST_TAA_FULL_LOAD):
--   CALL INGEST_TAA_LAUNCH_FULL_LOAD(NULL, NULL, 'demo.FAB_CF_WS_N1_STG');
--   CALL INGEST_TAA_LAUNCH_FULL_LOAD('DCS_CF_FO_Test', NULL, 'demo.FAB_CF_WS_N1_STG');

-- MONITOR (live task status for the current run):
--   SELECT * FROM TABLE(TASK_DEPENDENTS('TAA_FL_ROOT', TRUE)) ORDER BY SCHEDULED_TIME;

-- VIEW HISTORY (last 20 runs):
--   SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'TAA_FL_ROOT', RESULT_LIMIT => 20))
--   ORDER BY SCHEDULED_TIME DESC;

-- VIEW ALL TASKS IN LAST RUN:
--   SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(RESULT_LIMIT => 100))
--   WHERE GRAPH_VERSION IS NOT NULL
--   ORDER BY SCHEDULED_TIME DESC;

-- CHECK CURRENT RUN PARAMETERS:
--   SELECT * FROM INGEST_TAA_RUN_CONFIG;

-- SUSPEND DAG (before making any changes):
--   ALTER TASK TAA_FL_ROOT SUSPEND;

-- RESUME DAG (after changes -- root task requires SCHEDULE to resume):
--   ALTER TASK TAA_FL_ROOT RESUME;

-- VIEW DAG GRAPH:
--   SELECT * FROM TABLE(TASK_DEPENDENTS('TAA_FL_ROOT', FALSE));


-- =================================================
-- ENABLE THE TASK DAG
-- =================================================

ALTER TASK TAA_FL_FINALIZE                         RESUME;
ALTER TASK TAA_FL_W2_TIMESLICEPOSTSHIFTDIFFDETAIL  RESUME;
ALTER TASK TAA_FL_W2_TIMESLICEPOSTEXCEPTIONDETAIL  RESUME;
ALTER TASK TAA_FL_W2_TIMEOFFREQUESTDETAIL          RESUME;
ALTER TASK TAA_FL_W2_USERINFOISSALARY              RESUME;
ALTER TASK TAA_FL_W1_TIMESLICEPOST                 RESUME;
ALTER TASK TAA_FL_W1_TIMEOFFREQUEST                RESUME;
ALTER TASK TAA_FL_W1_TIMEOFFDATA                   RESUME;
ALTER TASK TAA_FL_W1_USERINFO                      RESUME;
ALTER TASK TAA_FL_W1_SCHEDULE                      RESUME;
ALTER TASK TAA_FL_W1_PAYTYPE                       RESUME;
ALTER TASK TAA_FL_W1_ENTERPRISECUSTOMER            RESUME;
ALTER TASK TAA_FL_W1_CUSTOMER                      RESUME;
ALTER TASK TAA_FL_ROOT                             RESUME;
