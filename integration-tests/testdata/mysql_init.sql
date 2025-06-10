CREATE DATABASE test;
USE test;

CREATE TABLE supported_data_types
(
    tinyint_col TINYINT,
    tinyint_unsigned_col TINYINT UNSIGNED,
    smallint_col SMALLINT,
    smallint_unsigned_col SMALLINT UNSIGNED,
    integer_col INT,
    integer_unsigned_col INT UNSIGNED,
    mediumint_col MEDIUMINT,
    mediumint_unsigned_col MEDIUMINT UNSIGNED,
    bigint_col BIGINT,
    bigint_unsigned_col BIGINT UNSIGNED,
    float_col FLOAT,
    double_col DOUBLE,
    decimal_col DECIMAL(60, 10),
    date_col DATE,
    datetime_col DATETIME,
    time_col TIME,
    timestamp_col TIMESTAMP,
    year_col YEAR,
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    varchar_utf8_bin_col VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin,
    binary_col BINARY(10),
    varbinary_col VARBINARY(100),
    tinytext_col TINYTEXT,
    text_col TEXT,
    mediumtext_col MEDIUMTEXT,
    longtext_col LONGTEXT,
    tinyblob_col TINYBLOB,
    blob_col BLOB,
    mediumblob_col MEDIUMBLOB,
    longblob_col LONGBLOB,
    json_col JSON,
    geometry_col GEOMETRY
);

INSERT INTO supported_data_types values
(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1.1, 2.2, 3.33, '2025-03-14', '2025-03-14 17:36:25', '11:11:11', '2025-03-14 11:11:11', '1999', 'char', 'varchar', 'varchar_utf8_bin', X'01', X'02', 'tinytext', 'text', 'mediumtext', 'longtext', X'01', X'02', X'03', X'04', '{"key": "value"}', Point(15, 20)),
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE simple_table (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

INSERT INTO simple_table VALUES (1, 'Tom'), (2, 'Jerry'), (3, 'Spike');

CREATE TABLE empty_table (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
