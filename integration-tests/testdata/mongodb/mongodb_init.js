// Seed data for the MongoDB integration tests.
//
// This script runs from /docker-entrypoint-initdb.d on first start of the
// container (the test setup always starts from a fresh volume).
const test = db.getSiblingDB('test');

test.simple_table.drop();
test.simple_table.insertMany([
  { _id: 1, id: 1, name: 'Tom' },
  { _id: 2, id: 2, name: 'Jerry' },
  { _id: 3, id: 3, name: 'Spike' },
]);

// A collection keyed by ObjectId, so that `_id` type inference is exercised
// with MongoDB's default key type.
test.object_id_table.drop();
test.object_id_table.insertMany([
  { _id: ObjectId('507f1f77bcf86cd799439011'), name: 'Tom' },
  { _id: ObjectId('507f1f77bcf86cd799439012'), name: 'Jerry' },
]);

// Documents that use every supported BSON type, including nested documents and
// arrays, to check that the raw document column round-trips faithfully.
test.supported_data_types.drop();
test.supported_data_types.insertMany([
  {
    _id: 1,
    double_col: 1.5,
    int32_col: 2,
    int64_col: NumberLong(3),
    string_col: 'text',
    bool_col: true,
    date_col: ISODate('2024-01-02T03:04:05.000Z'),
    object_id_col: ObjectId('507f1f77bcf86cd799439011'),
    binary_col: BinData(0, 'AQI='),
    decimal_col: NumberDecimal('1.23'),
    object_col: { a: 1 },
    array_col: [1, 2],
    null_col: null,
  },
  {
    _id: 2,
    double_col: null,
    int32_col: null,
    int64_col: null,
    string_col: null,
    bool_col: null,
    date_col: null,
    object_id_col: null,
    binary_col: null,
    decimal_col: null,
    object_col: null,
    array_col: null,
    null_col: null,
  },
]);

// Starts empty: the insert tests declare the schema explicitly.
test.insert_supported_data_types.drop();
