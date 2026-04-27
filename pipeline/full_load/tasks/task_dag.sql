USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE TASK TAA_FULL_ROOT
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    SCHEDULE  = '11520 MINUTE'               -- 8 days: satisfies Snowflake RESUME requirement; task is always triggered manually via INGEST_TAA_LAUNCH_FULL_LOAD
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'TAA full load DAG root. Clears target tables and builds file manifest. Triggers all individual table tasks upon success. Always triggered via INGEST_TAA_LAUNCH_FULL_LOAD -- schedule exists only to satisfy RESUME requirement.'
AS
CALL INGEST_TAA_FULL_LOAD_PREPARE();


-- =============================================================================
-- TASKS  (12 tasks, all run in parallel immediately after ROOT)
-- =============================================================================


CREATE OR REPLACE TASK TAA_FULL_CUSTOMER
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: CUSTOMER'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('CUSTOMER');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: ENTERPRISECUSTOME'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_PAYTYPE
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: PAYTYPE'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('PAYTYPE');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_SCHEDULE
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: SCHEDULE'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('SCHEDULE');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_USERINFO
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: USERINFO'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('USERINFO');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: TIMEOFFDATA'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: TIMEOFFREQUEST'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: TIMESLICEPOST'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST');


-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: USERINFOISSALARY'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: TIMEOFFREQUESTDETAIL'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: TIMESLICEPOSTEXCEPTIONDETAIL'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: TIMESLICEPOSTSHIFTDIFFDETAIL'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: USERINFOPAYROLLMAPPING'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_LLDETAIL
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: LLDETAIL'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('LLDETAIL');

-- -----------------------------------------------------------------------------


CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load: USERINFOEMPSTATUS'
    AFTER TAA_FULL_ROOT
AS
CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS');


-- =============================================================================
-- FINALIZE TASK
-- Runs after all 15 leaf tasks.
-- =============================================================================


CREATE OR REPLACE TASK TAA_FULL_FINALIZE
    WAREHOUSE = &{{SNOWFLAKE_WAREHOUSE}}
    COMMENT = 'TAA full load finalize: updates INGEST_TAA_FULL_LOAD_STATE after all 15 leaf tasks succeed.'
    AFTER
        TAA_FULL_CUSTOMER,
        TAA_FULL_ENTERPRISECUSTOMER,
        TAA_FULL_PAYTYPE,
        TAA_FULL_SCHEDULE,
        TAA_FULL_TIMEOFFDATA,
        TAA_FULL_USERINFO,
        TAA_FULL_USERINFOISSALARY,
        TAA_FULL_TIMEOFFREQUEST,
        TAA_FULL_TIMEOFFREQUESTDETAIL,
        TAA_FULL_TIMESLICEPOST,
        TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL,
        TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL,
        TAA_FULL_USERINFOPAYROLLMAPPING,
        TAA_FULL_LLDETAIL,
        TAA_FULL_USERINFOEMPSTATUS
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
--   SELECT * FROM TABLE(TASK_DEPENDENTS('TAA_FULL_ROOT', TRUE)) ORDER BY SCHEDULED_TIME;

-- VIEW HISTORY (last 20 runs):
--   SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'TAA_FULL_ROOT', RESULT_LIMIT => 20))
--   ORDER BY SCHEDULED_TIME DESC;

-- VIEW ALL TASKS IN LAST RUN:
--   SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(RESULT_LIMIT => 100))
--   WHERE GRAPH_VERSION IS NOT NULL
--   ORDER BY SCHEDULED_TIME DESC;

-- CHECK CURRENT RUN PARAMETERS:
--   SELECT * FROM INGEST_TAA_RUN_CONFIG;

-- SUSPEND DAG (before making any changes):
--   ALTER TASK TAA_FULL_ROOT SUSPEND;

-- RESUME DAG (after changes -- root task requires SCHEDULE to resume):
--   ALTER TASK TAA_FULL_ROOT RESUME;

-- VIEW DAG GRAPH:
--   SELECT * FROM TABLE(TASK_DEPENDENTS('TAA_FULL_ROOT', FALSE));


-- =================================================
-- ENABLE THE TASK DAG
-- =================================================

ALTER TASK TAA_FULL_FINALIZE                      RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL  RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL  RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL          RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY              RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST                 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST                RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA                   RESUME;
ALTER TASK TAA_FULL_USERINFO                      RESUME;
ALTER TASK TAA_FULL_SCHEDULE                      RESUME;
ALTER TASK TAA_FULL_PAYTYPE                       RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER            RESUME;
ALTER TASK TAA_FULL_CUSTOMER                      RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING        RESUME;
ALTER TASK TAA_FULL_LLDETAIL                      RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS             RESUME;
ALTER TASK TAA_FULL_ROOT                          RESUME;
