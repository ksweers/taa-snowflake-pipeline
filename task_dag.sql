USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE TASK TAA_DL_ROOT
    WAREHOUSE = WH_DS_AUTOMATION_TST
    SCHEDULE  = 'USING CRON 0 2 * * * UTC'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'TAA delta load DAG root. Builds STAGE_TAA_DELTA_MANIFEST. Triggers all Wave 1 tasks upon success. Fires nightly at 02:00 UTC; also triggered on-demand via INGEST_TAA_LAUNCH_DELTA_LOAD.'
AS
CALL INGEST_TAA_DELTA_PREPARE();


-- =============================================================================
-- TABLE TASKS  (12 tasks, all run in parallel immediately after ROOT)
-- =============================================================================


CREATE OR REPLACE TASK TAA_DL_W1_CUSTOMER
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load Wave 1: CUSTOMER.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DL_W1_ENTERPRISECUSTOMER
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load Wave 1: ENTERPRISECUSTOMER.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DL_W1_PAYTYPE
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load Wave 1: PAYTYPE.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DL_W1_SCHEDULE
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load Wave 1: SCHEDULE.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DL_W1_USERINFO
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load: USERINFO.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('USERINFO');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DL_W1_TIMEOFFDATA
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load Wave 1: TIMEOFFDATA.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DL_W1_TIMEOFFREQUEST
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load: TIMEOFFREQUEST.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DL_W1_TIMESLICEPOST
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load: TIMESLICEPOST.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST');


CREATE OR REPLACE TASK TAA_DL_W2_USERINFOISSALARY
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load: USERINFOISSALARY.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DL_W2_TIMEOFFREQUESTDETAIL
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load: TIMEOFFREQUESTDETAIL.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DL_W2_TIMESLICEPOSTEXCEPTIONDETAIL
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load: TIMESLICEPOSTEXCEPTIONDETAIL.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DL_W2_TIMESLICEPOSTSHIFTDIFFDETAIL
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load: TIMESLICEPOSTSHIFTDIFFDETAIL.'
    AFTER TAA_DL_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL');


-- =============================================================================
-- FINALIZE TASK
-- Runs after all 12 table tasks complete.
-- =============================================================================


CREATE OR REPLACE TASK TAA_DL_FINALIZE
    WAREHOUSE = WH_DS_AUTOMATION_TST
    COMMENT = 'TAA delta load finalize: logs run summary after all 12 table tasks succeed.'
    AFTER
        TAA_DL_W1_CUSTOMER,
        TAA_DL_W1_ENTERPRISECUSTOMER,
        TAA_DL_W1_PAYTYPE,
        TAA_DL_W1_SCHEDULE,
        TAA_DL_W1_TIMEOFFDATA,
        TAA_DL_W1_USERINFO,
        TAA_DL_W1_TIMEOFFREQUEST,
        TAA_DL_W1_TIMESLICEPOST,
        TAA_DL_W2_USERINFOISSALARY,
        TAA_DL_W2_TIMEOFFREQUESTDETAIL,
        TAA_DL_W2_TIMESLICEPOSTEXCEPTIONDETAIL,
        TAA_DL_W2_TIMESLICEPOSTSHIFTDIFFDETAIL
AS
CALL INGEST_TAA_DELTA_FINALIZE();


-- =============================================================================
-- ENABLE THE TASK DAG
-- =============================================================================
-- Resume order: leaf tasks first, root task last.
-- Snowflake requires children to be RESUMED before the root can be RESUMED.
-- =============================================================================



-- =============================================================================
-- QUICK REFERENCE
-- =============================================================================

-- FIRST-TIME SETUP (sets STAGE_NAME and triggers immediately):
--   CALL INGEST_TAA_LAUNCH_DELTA_LOAD(NULL, NULL, 'demo.FAB_CF_WS_N1_STG');

-- ON-DEMAND TRIGGER (uses persisted STAGE_NAME -- no stage arg needed after first setup):
--   CALL INGEST_TAA_LAUNCH_DELTA_LOAD();

-- ON-DEMAND WITH OVERRIDE (client-scoped, uses persisted STAGE_NAME):
--   CALL INGEST_TAA_LAUNCH_DELTA_LOAD('DCS_CF_FO_Test');

-- MONITOR (live task status for the current run):
--   SELECT * FROM TABLE(TASK_DEPENDENTS('TAA_DL_ROOT', TRUE)) ORDER BY SCHEDULED_TIME;

-- VIEW HISTORY (last 20 runs):
--   SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'TAA_DL_ROOT', RESULT_LIMIT => 20))
--   ORDER BY SCHEDULED_TIME DESC;

-- CHECK CURRENT RUN PARAMETERS:
--   SELECT * FROM INGEST_TAA_DELTA_RUN_CONFIG;

-- CHANGE SCHEDULE (e.g. 03:00 EST = 08:00 UTC):
--   ALTER TASK TAA_DL_ROOT SUSPEND;
--   ALTER TASK TAA_DL_ROOT SET SCHEDULE = 'USING CRON 0 8 * * * UTC';
--   ALTER TASK TAA_DL_ROOT RESUME;

-- SUSPEND DAG (before making changes):
--   ALTER TASK TAA_DL_ROOT SUSPEND;

-- RESUME DAG (after changes):
--   ALTER TASK TAA_DL_ROOT RESUME;

-- VIEW DAG GRAPH:
--   SELECT * FROM TABLE(TASK_DEPENDENTS('TAA_DL_ROOT', FALSE));


-- =================================================
-- ENABLE THE TASK DAG
-- =================================================

ALTER TASK TAA_DL_FINALIZE                             RESUME;
ALTER TASK TAA_DL_W2_TIMESLICEPOSTSHIFTDIFFDETAIL      RESUME;
ALTER TASK TAA_DL_W2_TIMESLICEPOSTEXCEPTIONDETAIL      RESUME;
ALTER TASK TAA_DL_W2_TIMEOFFREQUESTDETAIL              RESUME;
ALTER TASK TAA_DL_W2_USERINFOISSALARY                  RESUME;
ALTER TASK TAA_DL_W1_TIMESLICEPOST                     RESUME;
ALTER TASK TAA_DL_W1_TIMEOFFREQUEST                    RESUME;
ALTER TASK TAA_DL_W1_TIMEOFFDATA                       RESUME;
ALTER TASK TAA_DL_W1_USERINFO                          RESUME;
ALTER TASK TAA_DL_W1_SCHEDULE                          RESUME;
ALTER TASK TAA_DL_W1_PAYTYPE                           RESUME;
ALTER TASK TAA_DL_W1_ENTERPRISECUSTOMER                RESUME;
ALTER TASK TAA_DL_W1_CUSTOMER                          RESUME;
ALTER TASK TAA_DL_ROOT                                 RESUME;
