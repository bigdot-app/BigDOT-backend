const { snakeCase } = require('change-case');
const aws_functions = require('./aws-functions');
const { print, println } = require('./util-functions');
const kinesisSqlCompiler = require('./kinesis-analytics-sql-code-compiler');

function respondToDialogflow(res, message) {
  if (res != null) {
    return res.json({
      displayText: message,
      speech: message
    })
  }
}

async function start_logging_data(params, res) {
  var dl_name = snakeCase(params['dl-name']);
  console.log(`${params['dl-type']} => ${params['dl-name']}`);

  if (params['dl-type'] === "database") {
    // start recording to S3 and DynamoDB instance

    respondToDialogflow(res, `Currently creating a database instance called ${params['dl-name']}.`);

    await aws_functions.connect.IoT.to.DynamoDB(`bigdot-${dl_name}-dynamodb`);
    await aws_functions.connect.IoT.to.S3(`bigdot-${dl_name}-s3`);
  } else if (params['dl-type'] === "data stream") {
    // start recording to Kinesis Stream instance
    respondToDialogflow(res, `Currently creating a data stream instance called ${params['dl-name']}.`);

    await aws_functions.connect.IoT.to.KinesisStream(`bigdot-${dl_name}-stream-input`);
  } else {
    console.log("This shouldn't have happened!!");
  }
}

async function stop_logging_data(params, res) {
  var dl_name = snakeCase(params['dl-name']);
  console.log(`${params['dl-type']} => ${params['dl-name']}`);

  if (params['dl-type'] === "database") {
    // start recording to S3 and DynamoDB instance

    respondToDialogflow(res, `Currently deleting database instance called ${params['dl-name']}.`);

    await aws_functions.disconnect.DynamoDB.from.IoT(`bigdot-${dl_name}-dynamodb`),
    await aws_functions.disconnect.S3.from.IoT(`bigdot-${dl_name}-s3`)
  } else if (params['dl-type'] === "data stream") {
    // start recording to Kinesis Stream instance
    respondToDialogflow(res, `Currently delete data stream instance called ${params['dl-name']}.`);

    await aws_functions.disconnect.KinesisStream.from.IoT(`bigdot-${dl_name}-stream-input`);
  } else {
    console.log("This shouldn't have happened!!");
  }
}

async function start_stream_analysis(params, res) {
  var stream_name = snakeCase(params['stream-name']);
  var analysis_type = params['analysis-type'];
  var sensor_fields = params['sensor-fields'];

  var appCode;
  if (analysis_type === "basic statistics") {
    appCode = kinesisSqlCompiler.basic_stats(sensor_fields[0], 25)
  } else if (analysis_type === "anomaly") {
    appCode = kinesisSqlCompiler.anomaly_detect(sensor_fields);
  } else {
    appCode = undefined;
  }

  respondToDialogflow(res, `Preparing analysis of data stream ${params['stream-name']}...`);

  await aws_functions.create.KinesisAnalytics.application({
    appName: `bigdot-${stream_name}-app`,
    appCode: appCode,
    inputStreamName: `bigdot-${stream_name}-stream-input`,
    outputStreamName: `bigdot-${stream_name}-stream-output`
  });
}

async function stop_stream_analysis(params, res) {
  var stream_name = snakeCase(params['stream-name']);

  respondToDialogflow(res, `Stopping streaming analysis of data stream ${params['stream-name']}...`);

  await aws_functions.destroy.KinesisAnalytics.application(`bigdot-${stream_name}-app`);
}

module.exports = {
  start_logging_data,
  stop_logging_data,
  start_stream_analysis,
  stop_stream_analysis
};