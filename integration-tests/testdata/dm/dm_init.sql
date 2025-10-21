CREATE TABLE supported_data_types_test (
    bit_col BIT,
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    real_col REAL,
    float_col FLOAT,
    double_col DOUBLE,
    numeric_col NUMERIC(10, 2),
    decimal_col DECIMAL(10, 2),
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    text_col TEXT,
    binary_col BINARY(1),
    varbinary_col VARBINARY(10),
    image_col IMAGE,
    timestamp_col TIMESTAMP,
    time_col TIME(2),
    date_col DATE
);

INSERT INTO supported_data_types VALUES
(1, 1, 2, 3, 4, 1.1, 2.2, 3.3, 4.4, 5.5, 'char', 'varchar', 'text', X'01', X'02', UTL_RAW.CAST_TO_RAW('image'), TIMESTAMP '2002-12-12 09:10:21', '09:10:21.20', '2023-10-01'),
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
COMMIT;