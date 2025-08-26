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
(1, 2, 3, 1.1, 2.2, 3.3, 'char', 'varchar', 'bpchar', 'text', E'\\xDEADBEEF', '2023-10-01', '12:34:56', '3 months 2 weeks', TRUE, '{"key1": "value1"}', '{"key2": "value2"}', ST_GeomFromText('POINT(1 1)', 312), ARRAY[1, 2], ARRAY[3, 4], ARRAY[5, 6], ARRAY[1.1, 2.2], ARRAY[3.3, 4.4], ARRAY['char0', 'char1'], ARRAY['varchar0', 'varchar1'], ARRAY['bpchar0', 'bpchar1'], ARRAY['text0', 'text1'], ARRAY[true, false], '<item>1</item>', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


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

CREATE TABLE insert_table_with_primary_key (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
INSERT INTO insert_table_with_primary_key (name) VALUES ('Tom');



-- Create simplified timestamp test table
CREATE TABLE timestamp_test (
    -- Basic timestamp types
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMPTZ,
    
    -- Timestamps with different precision
    timestamp_0 TIMESTAMP(0),
    timestamp_3 TIMESTAMP(3),
    timestamp_6 TIMESTAMP(6)
);

-- Insert regular timestamp data
INSERT INTO timestamp_test VALUES (
    '2023-10-27 12:34:56',
    '2023-10-27 12:34:56+02',
    '2023-10-27 12:34:56',
    '2023-10-27 12:34:56.123',
    '2023-10-27 12:34:56.123456'
);

-- Insert pre-Unix epoch timestamp data
INSERT INTO timestamp_test VALUES (
    '1969-07-20 20:17:40',
    '1969-07-20 20:17:40+00',
    '1969-07-20 20:17:40',
    '1969-07-20 20:17:40.123',
    '1969-07-20 20:17:40.123456'
);

-- Insert future timestamp data
INSERT INTO timestamp_test VALUES (
    '2030-12-31 23:59:59',
    '2030-12-31 23:59:59+00',
    '2030-12-31 23:59:59',
    '2030-12-31 23:59:59.999',
    '2030-12-31 23:59:59.999999'
);

-- Insert precision test data
INSERT INTO timestamp_test VALUES (
    '2023-10-27 12:34:56.876543',
    '2023-10-27 12:34:56.876543+00',
    '2023-10-27 12:34:57',
    '2023-10-27 12:34:56.877',
    '2023-10-27 12:34:56.876543'
);

-- Insert minimum timestamp data
INSERT INTO timestamp_test VALUES (
    '0001-01-01 00:00:00',
    '0001-01-01 00:00:00+00',
    '0001-01-01 00:00:00',
    '0001-01-01 00:00:00.000',
    '0001-01-01 00:00:00.000000'
);

-- Insert maximum timestamp data
INSERT INTO timestamp_test VALUES (
    '9999-12-31 23:59:59',
    '9999-12-31 23:59:59+00',
    '9999-12-31 23:59:59',
    '9999-12-31 23:59:59.999',
    '9999-12-31 23:59:59.999999'
);
-- Insert NULL values
INSERT INTO timestamp_test VALUES (
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
);
