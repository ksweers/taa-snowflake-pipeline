USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE TASK TAA_DELTA_ROOT
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    SCHEDULE  = 'USING CRON 0 2 * * * UTC'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'TAA delta load DAG root. Builds STAGE_TAA_DELTA_MANIFEST. Triggers all individual table tasks upon success. Fires nightly at 02:00 UTC; also triggered on-demand via INGEST_TAA_LAUNCH_DELTA_LOAD.'
AS
CALL INGEST_TAA_DELTA_PREPARE();


-- =============================================================================
-- TASKS  (12 tasks, all run in parallel immediately after ROOT)
-- =============================================================================


CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: CUSTOMER'
    AFTER TAA_DELTA_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: ENTERPRISECUSTOMER'
    AFTER TAA_DELTA_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: PAYTYPE'
    AFTER TAA_DELTA_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: SCHEDULE.'
    AFTER TAA_DELTA_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DELTA_USERINFO
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: USERINFO'
    AFTER TAA_DELTA_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('USERINFO');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: TIMEOFFDATA'
    AFTER TAA_DELTA_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: TIMEOFFREQUEST'
    AFTER TAA_DELTA_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: TIMESLICEPOST'
    AFTER TAA_DELTA_ROOT
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST');


-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: USERINFOISSALARY'
    AFTER TAA_DELTA_USERINFO
AS
CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: TIMEOFFREQUESTDETAIL'
    AFTER TAA_DELTA_TIMEOFFREQUEST
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: TIMESLICEPOSTEXCEPTIONDETAIL'
    AFTER TAA_DELTA_TIMESLICEPOST
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load: TIMESLICEPOSTSHIFTDIFFDETAIL'
    AFTER TAA_DELTA_TIMESLICEPOST
AS
CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL');


-- =============================================================================
-- FINALIZE TASK
-- Runs after all 9 leaf tasks.
-- =============================================================================


CREATE OR REPLACE TASK TAA_DELTA_FINALIZE
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA delta load finalize: logs run summary after all 9 leaf tasks succeed.'
    AFTER
        TAA_DELTA_CUSTOMER,
        TAA_DELTA_ENTERPRISECUSTOMER,
        TAA_DELTA_PAYTYPE,
        TAA_DELTA_SCHEDULE,
        TAA_DELTA_TIMEOFFDATA,
        TAA_DELTA_USERINFOISSALARY,
        TAA_DELTA_TIMEOFFREQUESTDETAIL,
        TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL,
        TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL
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
--   SELECT * FROM TABLE(TASK_DEPENDENTS('TAA_DELTA_ROOT', TRUE)) ORDER BY SCHEDULED_TIME;

-- VIEW HISTORY (last 20 runs):
--   SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'TAA_DELTA_ROOT', RESULT_LIMIT => 20))
--   ORDER BY SCHEDULED_TIME DESC;

-- CHECK CURRENT RUN PARAMETERS:
--   SELECT * FROM INGEST_TAA_DELTA_RUN_CONFIG;

-- CHANGE SCHEDULE (e.g. 03:00 EST = 08:00 UTC):
--   ALTER TASK TAA_DELTA_ROOT SUSPEND;
--   ALTER TASK TAA_DELTA_ROOT SET SCHEDULE = 'USING CRON 0 8 * * * UTC';
--   ALTER TASK TAA_DELTA_ROOT RESUME;

-- SUSPEND DAG (before making changes):
--   ALTER TASK TAA_DELTA_ROOT SUSPEND;

-- RESUME DAG (after changes):
--   ALTER TASK TAA_DELTA_ROOT RESUME;

-- VIEW DAG GRAPH:
--   SELECT * FROM TABLE(TASK_DEPENDENTS('TAA_DELTA_ROOT', FALSE));


-- =================================================
-- ENABLE THE TASK DAG
-- =================================================

ALTER TASK TAA_DELTA_FINALIZE                          RESUME;
ALTER TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL      RESUME;
ALTER TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL      RESUME;
ALTER TASK TAA_DELTA_TIMEOFFREQUESTDETAIL              RESUME;
ALTER TASK TAA_DELTA_USERINFOISSALARY                  RESUME;
ALTER TASK TAA_DELTA_TIMESLICEPOST                     RESUME;
ALTER TASK TAA_DELTA_TIMEOFFREQUEST                    RESUME;
ALTER TASK TAA_DELTA_TIMEOFFDATA                       RESUME;
ALTER TASK TAA_DELTA_USERINFO                          RESUME;
ALTER TASK TAA_DELTA_SCHEDULE                          RESUME;
ALTER TASK TAA_DELTA_PAYTYPE                           RESUME;
ALTER TASK TAA_DELTA_ENTERPRISECUSTOMER                RESUME;
ALTER TASK TAA_DELTA_CUSTOMER                          RESUME;
ALTER TASK TAA_DELTA_ROOT                              RESUME;
