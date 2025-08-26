CREATE TABLE supported_data_types (
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    real_col REAL,
    double_col DOUBLE PRECISION,
    numeric_col NUMERIC(10, 2),

    char_col CHAR(10),
    varchar_col VARCHAR(255),
    bpchar_col BPCHAR,
    text_col TEXT,

    bytea_col BYTEA,

    date_col DATE,
    time_col TIME,
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMPTZ,
    interval_col INTERVAL,

    boolean_col BOOLEAN,
    json_col JSON,
    jsonb_col JSONB,
    geometry_col GEOMETRY,

    smallint_array_col SMALLINT[],
    integer_array_col INTEGER[],
    bigint_array_col BIGINT[],
    real_array_col REAL[],
    double_array_col DOUBLE PRECISION[],

    char_array_col CHAR(10)[],
    varchar_array_col VARCHAR(255)[],
    bpchar_array_col BPCHAR[],
    text_array_col TEXT[],

    bool_array_col BOOLEAN[],

    xml_col XML,
    uuid_col UUID
);

INSERT INTO supported_data_types VALUES
(1, 2, 3, 1.1, 2.2, 3.3, 'char', 'varchar', 'bpchar', 'text', E'\\xDEADBEEF', '2023-10-01', '12:34:56', '2023-10-01 12:34:56', '2023-10-01 12:34:56+00', '3 months 2 weeks', TRUE, '{"key1": "value1"}', '{"key2": "value2"}', ST_GeomFromText('POINT(1 1)', 312), ARRAY[1, 2], ARRAY[3, 4], ARRAY[5, 6], ARRAY[1.1, 2.2], ARRAY[3.3, 4.4], ARRAY['char0', 'char1'], ARRAY['varchar0', 'varchar1'], ARRAY['bpchar0', 'bpchar1'], ARRAY['text0', 'text1'], ARRAY[true, false], '<item>1</item>', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE simple_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

INSERT INTO simple_table VALUES (1, 'Tom'), (2, 'Jerry'), (3, 'Spike');

CREATE TABLE insert_supported_data_types (
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    real_col REAL,
    double_col DOUBLE PRECISION,
    numeric_col NUMERIC(10, 2),

    char_col CHAR(10),
    varchar_col VARCHAR(255),
    bpchar_col BPCHAR,
    text_col TEXT,

    bytea_col BYTEA,

    date_col DATE,
    time_col TIME,
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMPTZ,
    interval_col INTERVAL,

    boolean_col BOOLEAN,
    json_col JSON,
    jsonb_col JSONB,
    geometry_col GEOMETRY,

    smallint_array_col SMALLINT[],
    integer_array_col INTEGER[],
    bigint_array_col BIGINT[],
    real_array_col REAL[],
    double_array_col DOUBLE PRECISION[],

    char_array_col CHAR(10)[],
    varchar_array_col VARCHAR(255)[],
    bpchar_array_col BPCHAR[],
    text_array_col TEXT[],

    bool_array_col BOOLEAN[],

    xml_col XML,
    uuid_col UUID
);
INSERT INTO insert_supported_data_types VALUES (1);