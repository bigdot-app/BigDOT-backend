"use strict";

// initalize required modules

// require standard modules
const fs = require('fs');

// require remote modules
const express = require("express");
const bodyParser = require("body-parser");
const { snakeCase } = require('change-case');

// require local modules
const action_map = require('./action-functions');
const chart_functions = require('./chart-functions');
const { print, println, toSeconds } = require('./util-functions');

// create server instance
const server = require('express-async-await')(express());

// set up server-level middleware
server.use(bodyParser.json());

server.use(function (req, res, next) {
  println(`${req.method} called at starting epoch ${Date.now()}`);
  next();
});

server.use(function (err, req, res, next) {
  println(err);
  res.status(500).send(`Internal server error: ${err.message}`);
});

function retrieveBasicInfo(req) {
  let results = req.body.result;

  var contextObj = {};
  results.contexts.forEach(con => contextObj[con.name] = con.parameters);

  return {
    action: results.action,
    parameters: results.parameters,
    contexts: contextObj,
  }
}

var dataflowState = {
  iot2db: false,
  iot2stream: false,
  stream2analytics: false,
  iotThingName: process.env.AWS_IOT_THING_NAME,
  databaseName: "unknown",
  kinesisStreamName: "unknown",
  kinesisAnalyticsAppName: "unknown",
};

server.get("/graph-data/bar/analysis", async function(req, res, next) {
  var results = await chart_functions.get_bar_chart_data_analysis(snakeCase(dataflowState.kinesisStreamName));
  console.log(results);
  return res.json(results);
});

server.get("/graph-data/bar/groups/:groupType", async function(req, res, next) {
  var validEndpoints = ["accel", "accelerometer", "gyro", "gyroscope", "mag", "magnetometer", "misc", "miscellaneous"];
  if (validEndpoints.includes(req.params.groupType))
    return res.json(
      await chart_functions.get_bar_chart_data(req.params.groupType)
    );
  else
    return res.send(null);
});

server.get("/graph-data/time/:sensorType/:timeFromNow/", async function(req, res, next) {
  var finalSensorField;
  var sensorType = req.params.sensorType;
  var results;

  var dynamoDbTableName = snakeCase(dataflowState.databaseName);

  switch (sensorType) {
    case "temperature":
    case "humidity":
      results = await chart_functions.get_time_chart_data(
        dynamoDbTableName,
        [sensorType],
        toSeconds(req.params.timeFromNow)
      );
      finalSensorField = sensorType;
      break;
    case "barometer":
      results = await chart_functions.get_time_chart_data(
        dynamoDbTableName,
        ["pressure"],
        toSeconds(req.params.timeFromNow)
      );
      break;
    case "accelerometer":
    case "gyroscope":
    case "magnetometer":
      results = await chart_functions.get_time_chart_data(
        dynamoDbTableName,
        ['x', 'y', 'z'].map(axis => sensorType.substr(0,3) + '_' + axis),
        toSeconds(req.params.timeFromNow)
      );
      break;
    default:
      results = [null];
  }

  return res.json(results);
});

server.get("/data-flow", async function (req, res, next) {
  return res.json(dataflowState);
});

server.post("/dialogflow", async function(req, res, next) {
  let basicInfo = retrieveBasicInfo(req);
  println(JSON.stringify(basicInfo));

  // process via action map
  println(`Executing action '${basicInfo.action}'...`);
  await action_map[basicInfo.action](basicInfo.parameters, res);

  switch (basicInfo.action) {
    case "start_logging_data":
      if (basicInfo.parameters['dl-type'] === "database") {
        dataflowState.databaseName = basicInfo.parameters['dl-name'];
        dataflowState.iot2db = true;
      } else if (basicInfo.parameters['dl-type'] === "data stream") {
        dataflowState.kinesisStreamName = basicInfo.parameters['dl-name'];
        dataflowState.iot2stream = true;
      }
      break;
    case "stop_logging_data":
      if (basicInfo.parameters['dl-type'] === "database") {
        dataflowState.databaseName = "unknown";
        dataflowState.iot2db = false;
      } else if (basicInfo.parameters['dl-type'] === "data stream") {
        dataflowState.kinesisStreamName = "unknown";
        dataflowState.iot2stream = false;
      }
      break;
    case "start_stream_analysis":
      var streamName = basicInfo.parameters['stream-name'];
      dataflowState.stream2analytics = true;
      dataflowState.kinesisAnalyticsAppName = streamName;
      break;
    case "stop_stream_analysis":
      var streamName = basicInfo.parameters['stream-name'];
      dataflowState.stream2analytics = false;
      dataflowState.kinesisAnalyticsAppName = "unknown";
      break;
  }
});

// start listening
server.listen(process.env.PORT || 8080, () => console.log(`Server listening on port ${process.env.PORT || 8080}`));