CREATE TABLE "SYS"."supported_data_types"
(
    boolean_col BOOLEAN,
    smallint_col SMALLINT,
    integer_col INTEGER,
    binary_float_col BINARY_FLOAT,
    binary_double_col BINARY_DOUBLE,
    number_col NUMBER(10, 2),
    real_col REAL,
    float_col FLOAT,
    varchar2_col VARCHAR2(255),
    nvarchar2_col NVARCHAR2(255),
    char_col CHAR(10),
    nchar_col NCHAR(10),
    clob_col CLOB,
    nclob_col NCLOB,
    raw_col RAW(100),
    long_raw_col LONG RAW,
    blob_col BLOB,
    date_col DATE,
    timestamp_col TIMESTAMP
);

INSERT INTO "SYS"."supported_data_types" values (true, 1, 2, 1.1, 2.2, 3.3, 4.4, 5.5, 'varchar2', 'nvarchar2', 'char', 'nchar', 'clob', 'nclob', UTL_RAW.CAST_TO_RAW('raw'), UTL_RAW.CAST_TO_RAW('long raw'), UTL_RAW.CAST_TO_RAW('blob'), TO_DATE('2003/05/03 21:02:44', 'yyyy/mm/dd hh24:mi:ss'), TO_TIMESTAMP('2023-10-01 14:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.FF'));
INSERT INTO "SYS"."supported_data_types" values (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE "SYS"."supported_data_types2"
(
    long_col LONG
);

INSERT INTO "SYS"."supported_data_types2" values ('long');
INSERT INTO "SYS"."supported_data_types2" values (NULL);

CREATE TABLE "SYS"."simple_table" (
    id INTEGER,
    name VARCHAR2(255)
);

INSERT INTO "SYS"."simple_table" VALUES (1, 'Tom');
INSERT INTO "SYS"."simple_table" VALUES (2, 'Jerry');
INSERT INTO "SYS"."simple_table" VALUES (3, 'Spike');

CREATE TABLE "SYS"."empty_table" (
    id INTEGER,
    name VARCHAR2(255)
);