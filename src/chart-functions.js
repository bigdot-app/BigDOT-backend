const { snakeCase } = require('change-case');
const aws_functions = require('./aws-functions');
const { print, println, secondsEpoch, toSeconds } = require('./util-functions');

async function get_bar_chart_data_analysis(streamName) {
  var results = await aws_functions.extract.from.KinesisStream(`bigdot-${streamName}-stream-output`);
  if (results.ANOMALY_SCORE) {
    // anomaly detection
    var anomaly_score = results.ANOMALY_SCORE;
    delete results.ANOMALY_EXPLANATION;
    delete results.ANOMALY_SCORE;
    return {
      ...results,
      anomaly_score
    };
  } else {
    var resultsCopy = JSON.parse(JSON.stringify(results));
    console.log(resultsCopy)
    var { count, average, maximum, minimum, sample_standard_dev } = resultsCopy;
    delete results.count;
    delete results.average;
    delete results.maximum;
    delete results.minimum;
    delete results.sample_standard_dev;

    var value = Object.values(results)[0];

    return {
      value, count, minimum,
      maximum, average, sample_standard_dev
    };
  }
}

async function get_bar_chart_data(groupType) {
  var results = await aws_functions.extract.from.IoT();
  switch (groupType) {
    case "accel":
    case "accelerometer":
      return {
        timestamp: results.timestamp,
        x: results.acc_x,
        y: results.acc_y,
        z: results.acc_z,
      };
    case "gyro":
    case "gyroscope":
      return {
        timestamp: results.timestamp,
        x: results.gyr_x,
        y: results.gyr_y,
        z: results.gyr_z,
      };
    case "mag":
    case "magnetometer":
      return {
        timestamp: results.timestamp,
        x: results.mag_x,
        y: results.mag_y,
        z: results.mag_z,
      };
    case "misc":
    case "miscellaneous":
      return {
        timestamp: results.timestamp,
        temperature: results.temperature,
        humidity: results.humidity,
        pressure: results.pressure,
      };
    default:
      return {
        timestamp: results.timestamp,
        temperature: results.temperature,
        humidity: results.humidity,
        pressure: results.pressure,
        accelerometer: {
          x: results.acc_x,
          y: results.acc_y,
          z: results.acc_z,
        },
        gyroscope: {
          x: results.gyr_x,
          y: results.gyr_y,
          z: results.gyr_z,
        },
        magnetometer: {
          x: results.mag_x,
          y: results.mag_y,
          z: results.mag_z,
        },
      };
  }
};

async function get_time_chart_data(tableName, sensorFields, secondsFromNow) {
  var { Items } = await aws_functions.extract.from.DynamoDB({
    tableName: `bigdot-${tableName}-dynamodb`,
    ConsistentRead: true,
    attributes: [ 'timestamp', ...sensorFields ],
    compareOP: 'GE',
    valueList: [ secondsEpoch() - secondsFromNow ],
  });

  var results = Items.map(item => ({ timestamp: item.timestamp, values: sensorFields.map(field => item[field]) }));
  results.sort((a, b) => a.timestamp - b.timestamp);

  var finalResults = [];
  for (var result of results)
    finalResults.push(...result.values);
  return finalResults;
}

module.exports = {
  get_bar_chart_data,
  get_time_chart_data,
  get_bar_chart_data_analysis
}