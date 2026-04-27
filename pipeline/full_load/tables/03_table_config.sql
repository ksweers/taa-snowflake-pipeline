USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE TABLE INGEST_TAA_TABLE_CONFIG (
    TABLE_NAME                VARCHAR(100)   NOT NULL,
    LOAD_PROCEDURE_NAME       VARCHAR(500)   NOT NULL,
    DELTA_LOAD_PROCEDURE_NAME VARCHAR(500),
    SOURCE_TABLE_ID           VARCHAR(36),
    LOAD_ORDER                NUMBER(10,0)   NOT NULL DEFAULT 10,
    IS_ACTIVE_FULL_LOAD       BOOLEAN        NOT NULL DEFAULT TRUE,
    IS_ACTIVE_DELTA_LOAD      BOOLEAN        NOT NULL DEFAULT TRUE,
    IS_MULTI_TENANT           BOOLEAN        NOT NULL DEFAULT TRUE,
    DESCRIPTION               VARCHAR(500),
    CONSTRAINT PK_INGEST_TAA_TABLE_CONFIG PRIMARY KEY (TABLE_NAME)
);

INSERT INTO INGEST_TAA_TABLE_CONFIG
    (TABLE_NAME, LOAD_PROCEDURE_NAME, DELTA_LOAD_PROCEDURE_NAME, SOURCE_TABLE_ID, LOAD_ORDER, IS_ACTIVE_FULL_LOAD, IS_ACTIVE_DELTA_LOAD, IS_MULTI_TENANT, DESCRIPTION)
VALUES
    ('CUSTOMER',
        'FULL_LOAD_CUSTOMER()',         'DELTA_LOAD_CUSTOMER()',
        'bf376338-3aaf-4306-9885-db20b386631c',
        10, TRUE, TRUE, FALSE,
        'Enterprise customer reference -- no CLIENT_ID column;'),

    ('USERINFOISSALARY',
        'FULL_LOAD_USERINFOISSALARY()',  'DELTA_LOAD_USERINFOISSALARY()',
        'ab18c18c-ccff-62b6-4975-156ffc566ef8',
        50, TRUE, TRUE, TRUE,
        'Employee salary flag history per client'),

    ('TIMEOFFREQUESTDETAIL',
        'FULL_LOAD_TIMEOFFREQUESTDETAIL()', 'DELTA_LOAD_TIMEOFFREQUESTDETAIL()',
        'f9e8cf07-8d4f-1c51-df47-da7de058a176',
        80, TRUE, TRUE, TRUE,
        'Time off request detail lines per client'),

    ('ENTERPRISECUSTOMER',
        'FULL_LOAD_ENTERPRISECUSTOMER()',    'DELTA_LOAD_ENTERPRISECUSTOMER()',
        '49f4e893-dbbd-280a-93b3-9edccba30424',
        15, TRUE, TRUE, FALSE,
        'Enterprise customer reference -- no DATABASEPHYSICALNAME column'),

    ('PAYTYPE',
        'FULL_LOAD_PAYTYPE()',               'DELTA_LOAD_PAYTYPE()',
        'f774054a-9744-5cbf-731e-1bdd7df870f7',
        20, TRUE, TRUE, TRUE,
        'Pay type definitions per client'),

    ('SCHEDULE',
        'FULL_LOAD_SCHEDULE()',              'DELTA_LOAD_SCHEDULE()',
        'f4830a1d-ae29-8044-7c71-6bd4b5779b70',
        30, TRUE, TRUE, TRUE,
        'Employee schedule records per client'),

    ('USERINFO',
        'FULL_LOAD_USERINFO()',              'DELTA_LOAD_USERINFO()',
        'c930ce7d-904e-31a5-156d-559bc63e4246',
        40, TRUE, TRUE, TRUE,
        'User/employee profile records per client'),

    ('TIMEOFFDATA',
        'FULL_LOAD_TIMEOFFDATA()',           'DELTA_LOAD_TIMEOFFDATA()',
        '99629826-fe8e-61a4-0371-e3b33791fd23',
        60, TRUE, TRUE, TRUE,
        'Time off accrual and usage records per client'),

    ('TIMEOFFREQUEST',
        'FULL_LOAD_TIMEOFFREQUEST()',        'DELTA_LOAD_TIMEOFFREQUEST()',
        'db78652a-b192-ed5c-b7fd-410e8e8eb47a',
        70, TRUE, TRUE, TRUE,
        'Time off request header records per client'),

    ('TIMESLICEPOST',
        'FULL_LOAD_TIMESLICEPOST()',         'DELTA_LOAD_TIMESLICEPOST()',
        '0b30f4a8-bf11-0296-664d-a6996e0dca32',
        100, TRUE, TRUE, TRUE,
        'Posted time slice (punch) records per client'),

    ('TIMESLICEPOSTEXCEPTIONDETAIL',
        'FULL_LOAD_TIMESLICEPOSTEXCEPTIONDETAIL()', 'DELTA_LOAD_TIMESLICEPOSTEXCEPTIONDETAIL()',
        'be6c4966-d75e-ef52-7460-75c736afbf26',
        110, TRUE, TRUE, TRUE,
        'Time slice exception detail records per client'),

    ('TIMESLICEPOSTSHIFTDIFFDETAIL',
        'FULL_LOAD_TIMESLICEPOSTSHIFTDIFFDETAIL()', 'DELTA_LOAD_TIMESLICEPOSTSHIFTDIFFDETAIL()',
        'bb67bf1f-a87a-1912-57fd-686aee5c7361',
        120, TRUE, TRUE, TRUE,
        'Time slice shift differential detail records per client'),

    ('USERINFOPAYROLLMAPPING',
        'FULL_LOAD_USERINFOPAYROLLMAPPING()',     'DELTA_LOAD_USERINFOPAYROLLMAPPING()',
        'f1b0a3f6-49a5-a942-2349-e2c4c7fb15fa',
        25, TRUE, TRUE, TRUE,
        'Employee payroll system mapping records per client'),

    ('LLDETAIL',
        'FULL_LOAD_LLDETAIL()',                  'DELTA_LOAD_LLDETAIL()',
        '46c059a2-1b66-97a0-6dbc-4b1bf1ca4219',
        35, TRUE, TRUE, TRUE,
        'Labor level detail code definitions per client'),

    ('USERINFOEMPSTATUS',
        'FULL_LOAD_USERINFOEMPSTATUS()',         'DELTA_LOAD_USERINFOEMPSTATUS()',
        'e1b6510c-9ad1-ba04-1c43-1c8345dc44b1',
        55, TRUE, TRUE, TRUE,
        'Employee status history records per client');

--
