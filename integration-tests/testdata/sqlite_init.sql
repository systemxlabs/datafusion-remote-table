CREATE TABLE supported_data_types (
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    int_col INTEGER,
    bigint_col BIGINT,
    int2_col INT2,
    int4_col INT4,
    int8_col INT8,
    float_col FLOAT,
    double_col DOUBLE,
    real_col REAL,
    real_precision_col REAL(10),
    real_precision_scale_col REAL (10, 2),
    numeric_col NUMERIC,
    numeric_precision_col NUMERIC(10),
    numeric_precision_scale_col NUMERIC(10, 2),
    char_col CHAR,
    char_len_col CHAR(10),
    varchar_col VARCHAR,
    varchar_len_col VARCHAR(120),
    text_col TEXT,
    text_len_col TEXT(200),
    binary_col BINARY,
    binary_len_col BINARY(10),
    varbinary_col VARBINARY,
    varbinary_len_col VARBINARY(200),
    blob_col BLOB
);

INSERT INTO supported_data_types VALUES
(1, 2, 3, 4, 5, 6, 7, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 'char', 'char(10)', 'varchar', 'varchar(120)', 'text', 'text(200)', X'01', X'02', X'03', X'04', X'05'),
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE simple_table (
    id INTEGER,
    name TEXT
);

INSERT INTO simple_table VALUES (1, 'Tom'), (2, 'Jerry'), (3, 'Spike');

CREATE TABLE insert_supported_data_types (
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    int_col INTEGER,
    bigint_col BIGINT,
    int2_col INT2,
    int4_col INT4,
    int8_col INT8,
    float_col FLOAT,
    double_col DOUBLE,
    real_col REAL,
    real_precision_col REAL(10),
    real_precision_scale_col REAL (10, 2),
    numeric_col NUMERIC,
    numeric_precision_col NUMERIC(10),
    numeric_precision_scale_col NUMERIC(10, 2),
    char_col CHAR,
    char_len_col CHAR(10),
    varchar_col VARCHAR,
    varchar_len_col VARCHAR(120),
    text_col TEXT,
    text_len_col TEXT(200),
    binary_col BINARY,
    binary_len_col BINARY(10),
    varbinary_col VARBINARY,
    varbinary_len_col VARBINARY(200),
    blob_col BLOB
);

CREATE TABLE insert_table_with_primary_key (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR
);

CREATE TABLE insert_exec_serialization_table (
    id INTEGER,
    name VARCHAR
);