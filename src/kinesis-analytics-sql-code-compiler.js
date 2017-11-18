function compile_basic_statistics(sensorField, maxRows) {
  if (![ 'humidity', 'temperature', 'barometer' ].includes(sensorField))
    throw new Error(`Invalid sensor field ${sensorField}!`);
  if (maxRows < 2)
    throw new Error(`Maximum rows must be greater than or equal to 2. Number given: ${maxRows}`);
  if (sensorField === "barometer")
    sensorField = "pressure";

  return `CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (
  "${sensorField}" REAL,
  "count" INTEGER,
  "average" REAL,
  "maximum" REAL,
  "minimum" REAL,
  "sample_standard_dev" REAL
);
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
SELECT STREAM "${sensorField}",
  COUNT(*) OVER SLIDING_ROW_WINDOW AS "count",
  MAX("${sensorField}") OVER SLIDING_ROW_WINDOW AS "maximum",
  MIN("${sensorField}") OVER SLIDING_ROW_WINDOW AS "minimum",
  AVG("${sensorField}") OVER SLIDING_ROW_WINDOW AS "average",
  STDDEV_SAMP("${sensorField}") OVER SLIDING_ROW_WINDOW AS "sample_standard_dev"
FROM "SOURCE_SQL_STREAM_001"
WINDOW SLIDING_ROW_WINDOW AS (ROWS ${maxRows - 1} PRECEDING);`
}

function compile_anomaly_detection(sensorFields) {
  if (typeof sensorFields === "undefined")
    throw new Error('No sensor fields provided!');
  if (typeof sensorFields === "string")
    sensorFields = [sensorFields];
  for (var sensorField of sensorFields) {
    if (![ 'humidity', 'temperature', 'barometer' ].includes(sensorField))
      throw new Error(`Invalid sensor field ${sensorField}!`);
  }

  delete sensorFields.barometer;
  sensorFields.push('pressure');

  return `CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (
  ${test.map(i => `"${i}" REAL`).join(', ')}
  "ANOMALY_SCORE" REAL, "ANOMALY_EXPLANATION" VARCHAR(20480)
);
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
SELECT STREAM ${sensorFields.map(field => `"${field}"`).join(", ")}, "ANOMALY_SCORE", "ANOMALY_EXPLANATION" FROM
  TABLE (
    RANDOM_CUT_FOREST_WITH_EXPLANATION (
      CURSOR (SELECT STREAM ${sensorFields.map(field => `"${field}"`).join(", ")} FROM "SOURCE_SQL_STREAM_001"),
      100,
      256,
      100000,
      1,
      false
    )
  ) WHERE "ANOMALY_SCORE" > 0`
}

module.exports = {
  basic_stats: compile_basic_statistics,
  anomaly_detect: compile_anomaly_detection
};