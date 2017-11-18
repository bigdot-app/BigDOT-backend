const { print, println } = require('./util-functions');

const defaultInputSchema = require('./kinesis-analytics-input-schema.json');

var AWS = require('aws-sdk');

var iot = new AWS.Iot();
var iotData = new AWS.IotData({ endpoint: process.env.AWS_IOT_ENDPOINT });
var dynamodb = new AWS.DynamoDB();
var docClient = new AWS.DynamoDB.DocumentClient();
var s3 = new AWS.S3();
var kinesis = new AWS.Kinesis();
var kinesisanalytics = new AWS.KinesisAnalytics();

AWS.config.update({region: process.env.AWS_REGION});

function sleep(time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

/*
 * The following "initial" assumptions will be made for the sake of the AWS IoT Challenge
 *  - The IoT thing is already defined and set up to broadcast data to AWS IoT
 *  - A topic rule is already created for the thing, but no actions are defined for it
 *  - The IAM Role generated for AWS IoT and Kinesis Stream have full admin access to
 *    IoT, S3, DynamoDB, Kinesis Streams, and Kinesis Analytics
 *    TODO: Auto generate appropriate roles and attach or verify attachemnt before starting the web server
 */

var defaultSleepDuration = 500;
var defaultMaxAttempts = 120;

async function _waitForExist(awsRequestFunc) {
  var attempts = 0;
  while (attempts < defaultMaxAttempts) {
    if (await awsRequestFunc())
      return true;
    else {
      await sleep(defaultSleepDuration);
      attempts++;
    }
  }

  return false;
}

async function _waitForNotExist(awsRequestFunc) {
  var attempts = 0;
  while (attempts < defaultMaxAttempts) {
    try {
      await awsRequestFunc();
    } catch (error) {
      return true;
    }

    await sleep(defaultSleepDuration);
    attempts++;
  }

  return false;
}

var waitFor = {
  DynamoDB: {
    table: {
      toExist: async function(tableName) {
        return await _waitForExist(
          () => dynamodb.describeTable({ TableName: tableName }).promise().then(details => details.Table.TableStatus === "ACTIVE")
        );
      },
      toNotExist: async function() {
        return await _waitForNotExist(
          () => dynamodb.describeTable({ TableName: tableName }).promise().then(details => console.log(details.Table.TableStatus))
        );
      }
    }
  },
  S3: {
    bucket: {
      toExist: async function(bucketName) {
        return await _waitForExist(
          () => s3.headBucket({ Bucket: bucketName }).promise().then(details => true)
        );
      },
      toNotExist: async function(bucketName) {
        return await _waitForNotExist(
          () => s3.headBucket({ Bucket: bucketName }).promise()
        );
      }
    }
  },
  KinesisStream: {
    toExist: async function(streamName) {
      return await _waitForExist(
        () => kinesis.describeStream({ StreamName: streamName }).promise()
          .then(details => details.StreamDescription.StreamStatus === "ACTIVE")
      );
    },
    toNotExist: async function(streamName) {
      return await _waitForNotExist(
        () => kinesis.describeStream({ StreamName: streamName }).promise()
      );
    }
  },
  KinesisAnalyticsApp: {
    toExist: async function(appName) {
      return await _waitForExist(
        () => kinesisanalytics.describeApplication({ ApplicationName: appName }).promise()
          .then(details => ['READY', 'RUNNING'].includes(details.ApplicationDetail.ApplicationStatus))
      );
    },
    toNotExist: async function(appName) {
      return await _waitForNotExist(
        () => kinesisanalytics.describeApplication({ ApplicationName: appName }).promise()
      );
    },
    toBeRunning: async function(appName) {
      return await _waitForExist(
        () => kinesisanalytics.describeApplication({ ApplicationName: appName }).promise()
          .then(details => details.ApplicationDetail.ApplicationStatus === "RUNNING")
      );
    },
    toBeNotRunning: async function(appName) {
      return await _waitForExist(
        () => kinesisanalytics.describeApplication({ ApplicationName: appName }).promise()
          .then(details => details.ApplicationDetail.ApplicationStatus === "READY")
      );
    },
  }
}

var aws_funcs = {
  connect: {
    IoT: {
      to: {
        DynamoDB: async function(tableName) {
          println(`Invoking function connect.IoT.to.DynamoDB with parameters ${Object.values(arguments)}`);
          const partitionKeyName = "timestamp";

          // check if table is created
          print(`Checking existance of table ${tableName}...`);
          try {
            await dynamodb.describeTable({ TableName: tableName }).promise();
            println('done!');
          } catch (error) {
            println('done!');

            // the table is non-existent, let's make it
            println(`Table ${tableName} does not exist!`);

            var params = {
              AttributeDefinitions: [
                { AttributeName: partitionKeyName, AttributeType: "N" }
              ],
              KeySchema: [
                { AttributeName: partitionKeyName, KeyType: "HASH" }
              ],
              ProvisionedThroughput: {
                ReadCapacityUnits: 5,
                WriteCapacityUnits: 5
              },
              TableName: tableName
            };

            print(`Creating table ${tableName}...`);
            await dynamodb.createTable(params).promise();
            println('done!');

            // wait until the table is created
            print('Waiting for the table to truly exist...');
            console.log(await waitFor.DynamoDB.table.toExist(tableName));
            // await dynamodb.waitFor('tableExists', { TableName: tableName }).promise();
            println('verified!');
          }

          println(`Table ${tableName} does exist!`);

          // check if the IoT rule's action is associated with the table
          print('Searching for action association between DynamoDB table and IoT thing...')
          var ruleResults = await iot.getTopicRule({ ruleName: process.env.AWS_IOT_TOPIC_RULE_NAME }).promise();
          // rule does exist, let's modify it
          for (var action of ruleResults.rule.actions) {
            if (action.dynamoDBv2 && action.dynamoDBv2.putItem.tableName === tableName) {
              // the action is associated, we are done
              println(`found!\nIoT thing ${process.env.AWS_IOT_THING_NAME} is connected to DynamoDB table ${tableName}.`);
              return 0;
            }
          }

          // the action isn't associated, let's fix that
          print(`not found! Fixing...`);
          var params = {
            ruleName: ruleResults.rule.ruleName,
            topicRulePayload: {
              actions: [
                ...ruleResults.rule.actions,
                {
                  dynamoDBv2: {
                    putItem: { tableName: tableName },
                    roleArn: process.env.AWS_ADMIN_ROLE_ARN
                  }
                }
              ],
              sql: ruleResults.rule.sql,
              awsIotSqlVersion: ruleResults.rule.awsIotSqlVersion,
              ruleDisabled: ruleResults.rule.ruleDisabled
            }
          };

          await iot.replaceTopicRule(params).promise();
          println(`done!`);
          return 0;
        },
        S3: async function(bucketName) {
          println(`Invoking function connect.IoT.to.S3 with parameters ${Object.values(arguments)}`);

          // check if bucket is created
          print(`Checking existance of bucket ${bucketName}...`);
          try {
            await s3.headBucket({ Bucket: bucketName }).promise();
            println('done!');
          } catch (error) {
            println('done!');
            // the bucket is non-existent, let's make it
            println(`Bucket ${bucketName} does not exist!`);

            print(`Creating bucket ${bucketName}...`);
            await s3.createBucket({ Bucket: bucketName }).promise();
            println('done!');

            // wait until the bucket is created
            print('Waiting for the bucket to truly exist...');
            await waitFor.S3.bucket.toExist(bucketName);
            println('verified!');
          }

          println(`Bucket ${bucketName} does exist!`);

          // Attach policy to bucket
          var params = {
            Bucket: bucketName,
            // TODO: Make policy more restricting
            Policy: JSON.stringify({
              "Version": "2012-10-17",
              "Statement": [
                {
                  "Action": "s3:*",
                  "Effect": "Allow",
                  "Resource": `arn:aws:s3:::${bucketName}/*`,
                  "Principal": "*"
                }
              ]
            })
          };

          print(`Attaching policy to bucket ${bucketName}...`);
          await s3.putBucketPolicy(params).promise();
          println('done!')

          // check if the IoT rule's action is associated with the bucket
          var params = { ruleName: process.env.AWS_IOT_TOPIC_RULE_NAME };

          print('Searching for action association between S3 bucket and IoT thing...')
          var ruleResults = await iot.getTopicRule(params).promise();
          // rule does exist, let's modify it
          for (var action of ruleResults.rule.actions) {
            if (action.s3 && action.s3.bucketName === bucketName) {
              // the action is associated, we are done
              println(`found!\nIoT thing ${process.env.AWS_IOT_THING_NAME} is connected to S3 bucket ${bucketName}`);
              return;
            }
          }

          // the action isn't associated, let's fix that
          print(`not found! Fixing...`);
          var params = {
            ruleName: ruleResults.rule.ruleName,
            topicRulePayload: {
              actions: [
                ...ruleResults.rule.actions,
                {
                  s3: {
                    bucketName: bucketName,
                    key: '${topic(3)}/${timestamp()}.json',
                    roleArn: process.env.AWS_ADMIN_ROLE_ARN,
                  }
                }
              ],
              sql: ruleResults.rule.sql,
              awsIotSqlVersion: ruleResults.rule.awsIotSqlVersion,
              ruleDisabled: ruleResults.rule.ruleDisabled
            }
          };

          await iot.replaceTopicRule(params).promise();
          println('done!');
          return 0;
        },
        KinesisStream: async function(streamName) {
          println('Invoking function connect.IoT.to.KinesisStream with parameters ' + Object.values(arguments));

          // check if kinesis stream is created
          print(`Checking existance of Kinesis stream ${streamName}...`);
          try {
            await kinesis.describeStream({ StreamName: streamName }).promise();
            println('done!');
          } catch (error) {
            println('done!');

            // the kinesis stream is non-existent, let's make it
            println(`Kinesis stream ${streamName} does not exist!`);

            var params = {
              ShardCount: 1,
              StreamName: streamName
            };

            print(`Creating Kinesis stream ${streamName}...`);
            await kinesis.createStream(params).promise();
            println('done!');

            // wait until the stream is created
            print('Waiting for the Kinesis stream to truly exist...');
            await waitFor.KinesisStream.toExist(streamName);
            println('done');
          }

          println(`Kinesis stream ${streamName} does exist!`);

          // check if the IoT rule's action is associated with the bucket
          var params = { ruleName: process.env.AWS_IOT_TOPIC_RULE_NAME };

          print('Searching for action association between Kinesis stream and IoT thing...')
          var ruleResults = await iot.getTopicRule(params).promise();
          // rule does exist, let's modify it
          for (var action of ruleResults.rule.actions) {
            if (action.kinesis && action.kinesis.streamName === streamName) {
              // the action is associated, we are done
              println(`found!\nIoT thing ${process.env.AWS_IOT_THING_NAME} is connected to stream ${streamName}`);
              return;
            }
          }

          // the action isn't associated, let's fix that
          print(`not found! Fixing...`);
          var params = {
            ruleName: ruleResults.rule.ruleName,
            topicRulePayload: {
              actions: [
                ...ruleResults.rule.actions,
                {
                  kinesis: {
                    roleArn: process.env.AWS_ADMIN_ROLE_ARN,
                    streamName: streamName,
                    partitionKey: '${newuuid()}'
                  },
                }
              ],
              sql: ruleResults.rule.sql,
              awsIotSqlVersion: ruleResults.rule.awsIotSqlVersion,
              ruleDisabled: ruleResults.rule.ruleDisabled
            }
          };

          await iot.replaceTopicRule(params).promise();
          println('done!');
          return 0;
        }
      }
    }
  },
  create: {
    KinesisAnalytics: {
      application: async function(options) {
        println('Invoking function create.KinesisAnalytics.application with parameters ' + Object.values(arguments));

        var appName = options.appName;
        var appCode = options.appCode;
        var inputStreamName = options.inputStreamName;
        var outputStreamName = options.outputStreamName;

        // check if an app with the same name already exists
        print(`Checking existance of Kinesis Analytics app ${appName}...`);
        try {
          await kinesisanalytics.describeApplication({ ApplicationName: appName }).promise();
          println('done!');
        } catch (error) {
          println('done!');

          // the Kinesis Analytics app is non-existent, let's make it
          println(`Kinesis Analytics app ${appName} does not exist!`);

          // first create output stream if needed

          // check if kinesis stream is created
          print(`Checking existance of Kinesis stream ${outputStreamName}...`);
          try {
            await kinesis.describeStream({ StreamName: outputStreamName }).promise();
            println('done!');
          } catch (error) {
            println('done!');

            // the kinesis stream is non-existent, let's make it
            println(`Kinesis stream ${outputStreamName} does not exist!`);

            var params = {
              ShardCount: 1,
              StreamName: outputStreamName
            };

            print(`Creating Kinesis stream ${outputStreamName}...`);
            await kinesis.createStream(params).promise();
            println('done!');

            // wait until the stream is created
            print('Waiting for the Kinesis stream to truly exist...');
            await waitFor.KinesisStream.toExist(outputStreamName);
            println('done');
          }

          println(`Kinesis stream ${outputStreamName} does exist!`);


          // retrieve both input and output kinesis stream ARNs
          var inputStreamArn = await kinesis.describeStream({ StreamName: inputStreamName }).promise().then(details => details.StreamDescription.StreamARN);
          var outputStreamArn = await kinesis.describeStream({ StreamName: outputStreamName }).promise().then(details => details.StreamDescription.StreamARN);

          var params = {
            ApplicationName: appName,
            ApplicationCode: appCode,
            Inputs: [
              {
                InputSchema: defaultInputSchema,
                NamePrefix: 'SOURCE_SQL_STREAM',
                KinesisStreamsInput: {
                  ResourceARN: inputStreamArn,
                  RoleARN: process.env.AWS_ADMIN_ROLE_ARN
                }
              },
            ],
            Outputs: [
              {
                DestinationSchema: {
                  RecordFormatType: 'JSON'
                },
                Name: 'DESTINATION_SQL_STREAM',
                KinesisStreamsOutput: {
                  ResourceARN: outputStreamArn,
                  RoleARN: process.env.AWS_ADMIN_ROLE_ARN
                }
              }
            ]
          };

          print(`Creating Kinesis Analytics app ${appName}...`);
          await kinesisanalytics.createApplication(params).promise();
          println('done!');

          // wait until the stream is created
          print('Waiting for the Kinesis Analytics app to truly exist...');
          await waitFor.KinesisAnalyticsApp.toExist(appName);
          println('done');
        }

        println(`Kinesis Analytics app ${appName} does exist!`);

        // check if the app is running
        var appDetails = await kinesisanalytics.describeApplication({ ApplicationName: appName }).promise()
          .then(details => details.ApplicationDetail);
        var appStatus = appDetails.ApplicationStatus;
        var inputId = appDetails.InputDescriptions[0].InputId;

        if (appStatus === "RUNNING") {
          // we are done here
          return 0;
        } else if (appStatus === "READY") {
          // let's start it
          var params = {
            ApplicationName: appName,
            InputConfigurations: [
              {
                Id: inputId,
                InputStartingPositionConfiguration: { InputStartingPosition: 'NOW' }
              }
            ]
          };
          print(`Starting Kinesis Analytics app ${appName}...`);
          await kinesisanalytics.startApplication(params).promise();
          println('done!');

          print('Waiting for the Kinesis Analytics app to be truly running...');
          await waitFor.KinesisAnalyticsApp.toBeRunning(appName);
          println('done');
        } else {
          // something went wrong
          throw new Error(`Kinesis Analytics app ${appName} has this state: '${appStatus}'!`);
        }
      }
    }
  },
  destroy: {
    KinesisAnalytics: {
      application: async function(appName) {
        println('Invoking function destroy.KinesisAnalytics.application with parameters ' + Object.values(arguments));

        // check if an app with the same name already exists
        print(`Checking existance of Kinesis Analytics app ${appName}...`);
        var appDetails, outputStreamName;
        try {
          appDetails = await kinesisanalytics.describeApplication({ ApplicationName: appName }).promise().then(details => details.ApplicationDetail);
          outputStreamName = appDetails.OutputDescriptions[0].KinesisStreamsOutputDescription.ResourceARN.split('/')[1];
          println('done!');
        } catch (error) {
          println('done!');

          // the Kinesis Analytics app is non-existent, let's make it
          println(`Kinesis Analytics app ${appName} does not exist!`);
          return 0;
        }

        // the application still exist, let's stop and delete it
        print(`Deleting Kinesis Analytics application ${appName}...`);
        await kinesisanalytics.deleteApplication({
          ApplicationName: appName,
          CreateTimestamp: appDetails.CreateTimestamp
        }).promise();
        println('done!');

        // wait until the analytics app doesn't exist
        print('Waiting for the Kinesis Analytics application to truly not exist...');
        await waitFor.KinesisAnalyticsApp.toNotExist(appName);
        println('done!');

        if (outputStreamName) {
          // check if output kinesis stream exists
          print(`Checking existance of output Kinesis stream ${outputStreamName}...`);
          try {
            await kinesis.describeStream({ StreamName: outputStreamName }).promise();
            println('done!');
          } catch (error) {
            // it doesn't exist, we are done here
            println('done!');
            println(`Kinesis stream ${outputStreamName} does not exist!`);
            return 0;
          }

          // it does exist, let's delete it
          print(`Kinesis stream ${outputStreamName} does exist! Deleting...`);
          await kinesis.deleteStream({ StreamName: outputStreamName }).promise();
          println('done!');

          // wait until the Kinesis stream is deleted
          print('Waiting for the Kinesis stream to truly not exist...');
          await waitFor.KinesisStream.toNotExist(outputStreamName);
          println('verified!');
        }
      }
    }
  },
  disconnect: {
    DynamoDB: {
      from: {
        IoT: async function(tableName) {
          println(`Invoking function disconnect.DynamoDB.from.IoT with parameters ${Object.values(arguments)}`);

          // check if the IoT rule's action is associated with the table
          print('Searching for action association between DynamoDB table and IoT thing...')
          var ruleResults = await iot.getTopicRule({ ruleName: process.env.AWS_IOT_TOPIC_RULE_NAME }).promise();
          // rule does exist, let's modify it
          for (var index in ruleResults.rule.actions) {
            var action = ruleResults.rule.actions[index];
            if (action.dynamoDBv2 && action.dynamoDBv2.putItem.tableName === tableName) {
              // the action is associated, let's remove it
              println('found!');
              print(`Deleting action association between IoT thing ${process.env.AWS_IOT_THING_NAME} and DynamoDB table ${tableName}...`);

              delete ruleResults.rule.actions[index];

              var params = {
                ruleName: ruleResults.rule.ruleName,
                topicRulePayload: {
                  actions: ruleResults.rule.actions,
                  sql: ruleResults.rule.sql,
                  awsIotSqlVersion: ruleResults.rule.awsIotSqlVersion,
                  ruleDisabled: ruleResults.rule.ruleDisabled
                }
              };

              await iot.replaceTopicRule(params).promise();
              println('done!');
              break;
            }
          }

          println('not found!');

          // the action isn't associated, moving on...

          // check if associated table still exists
          print(`Checking existance of table ${tableName}...`);
          try {
            await dynamodb.describeTable({ TableName: tableName }).promise();
            println('done!');
          } catch (error) {
            // it doesn't exist, we are done here
            println('done!');
            return 0;
          }

          // it still exists, let's delete it
          print(`Table ${tableName} does exist, deleting...`);
          await dynamodb.deleteTable({ TableName: tableName }).promise();
          println('done!');

          // wait until the table is deleted
          print('Waiting for the table to truly not exist...');
          await waitFor.DynamoDB.table.toNotExist(tableName);
          println('verified!');

          return 0;
        },
      }
    },
    S3: {
      from: {
        IoT: async function(bucketName) {
          println(`Invoking function disconnect.S3.from.IoT with parameters ${Object.values(arguments)}`);

          // check if the IoT rule's action is associated with the table
          print('Searching for action association between S3 bucket and IoT thing...');
          var ruleResults = await iot.getTopicRule({ ruleName: process.env.AWS_IOT_TOPIC_RULE_NAME }).promise();
          // rule does exist, let's modify it
          var isFound = false;
          for (var index in ruleResults.rule.actions) {
            var action = ruleResults.rule.actions[index];
            if (action.s3 && action.s3.bucketName === bucketName) {
              // the action is associated, let's remove it
              isFound = true;
              println('found!');
              print(`Deleting action association between IoT thing ${process.env.AWS_IOT_THING_NAME} and S3 bucket ${bucketName}...`);

              delete ruleResults.rule.actions[index];

              var params = {
                ruleName: ruleResults.rule.ruleName,
                topicRulePayload: {
                  actions: ruleResults.rule.actions,
                  sql: ruleResults.rule.sql,
                  awsIotSqlVersion: ruleResults.rule.awsIotSqlVersion,
                  ruleDisabled: ruleResults.rule.ruleDisabled
                }
              };

              await iot.replaceTopicRule(params).promise();
              println('done!');
              break;
            }
          }

          if (!isFound)
            println('not found!');

          // the action isn't associated, moving on...

          // check if bucket exists
          print(`Checking existance of bucket ${bucketName}...`);
          try {
            await s3.headBucket({ Bucket: bucketName }).promise();
            println('done!');
          } catch (error) {
            // it doesn't exist, we are done here
            println('done!');
            return 0;
          }

          // it still exists, let's delete it
          println(`Bucket ${bucketName} does exist!`);

          // we must first empty the bucket
          print('Listing the objects to be deleted...');
          var objList = await s3.listObjectsV2({ Bucket: bucketName }).promise().then(results => results.Contents);
          println('done!');

          if (objList.length > 0) {
            print('Emptying the bucket...');
            var params = {
              Bucket: bucketName,
              Delete: {
                Objects: objList.map(obj => ({ Key: obj.Key }) ),
                Quiet: false
              }
            };

            await s3.deleteObjects(params).promise();
            println('done!');
          } else {
            println('No objects found!');
          }

          print('Deleting the bucket...');
          await s3.deleteBucket({ Bucket: bucketName }).promise();
          println('done!');

          // wait until the bucket is deleted
          print('Waiting for the bucket to truly not exist...');
          await waitFor.S3.bucket.toNotExist(bucketName);
          println('verified!');

          return 0;
        }
      }
    },
    KinesisStream: {
      from: {
        IoT: async function(streamName) {
          println(`Invoking function disconnect.KinesisStream.from.IoT with parameters ${Object.values(arguments)}`);

          // check if the IoT rule's action is associated with the table
          print('Searching for action association between Kinesis stream and IoT thing...');
          var ruleResults = await iot.getTopicRule({ ruleName: process.env.AWS_IOT_TOPIC_RULE_NAME }).promise();
          // rule does exist, let's modify it
          var isFound = false;
          for (var index in ruleResults.rule.actions) {
            var action = ruleResults.rule.actions[index];
            if (action.kinesis && action.kinesis.streamName === streamName) {
              // the action is associated, let's remove it
              isFound = true;
              println('found!');
              print(`Deleting action association between IoT thing ${process.env.AWS_IOT_THING_NAME} and Kinesis stream ${streamName}...`);

              delete ruleResults.rule.actions[index];

              var params = {
                ruleName: ruleResults.rule.ruleName,
                topicRulePayload: {
                  actions: ruleResults.rule.actions,
                  sql: ruleResults.rule.sql,
                  awsIotSqlVersion: ruleResults.rule.awsIotSqlVersion,
                  ruleDisabled: ruleResults.rule.ruleDisabled
                }
              };

              await iot.replaceTopicRule(params).promise();
              println('done!');
              break;
            }
          }

          if (!isFound)
            println('not found!');

          // the action isn't associated, moving on...

          // check if kinesis stream exists
          print(`Checking existance of Kinesis stream ${streamName}...`);
          try {
            await kinesis.describeStream({ StreamName: streamName }).promise();
            println('done!');
          } catch (error) {
            // it doesn't exist, we are done here
            println('done!');
            println(`Kinesis stream ${streamName} does not exist!`);
            return 0;
          }

          // it does exist, let's delete it
          print(`Kinesis stream ${streamName} does exist! Deleting...`);
          await kinesis.deleteStream({ StreamName: streamName }).promise();
          println('done!');

          // wait until the Kinesis stream is deleted
          print('Waiting for the Kinesis stream to truly not exist...');
          await waitFor.KinesisStream.toNotExist(streamName);
          println('verified!');
        }
      }
    }
  },
  extract: {
    from: {
      IoT: async function() {
        var shadowData = await iotData.getThingShadow({ thingName: process.env.AWS_IOT_THING_NAME }).promise();
        var payload = JSON.parse(shadowData.payload);
        var results = {
          ...payload.state.reported,
          timestamp: payload.timestamp,
        };

        delete results.LED_value;
        return results;
      },
      DynamoDB: async function(options) {
        var params = {
          TableName : options.tableName,
          AttributesToGet: options.attributes || undefined,
          ScanFilter: {
            'timestamp': {
              ComparisonOperator: options.compareOP,
              AttributeValueList: options.valueList,
            },
          },
          Limit: options.limit || undefined,
        };

        return docClient.scan(params).promise();
      },
      KinesisStream: async function(streamName) {
        var shardId;
        try {
          shardId = await kinesis.describeStream({ StreamName: streamName }).promise().then(details => details.StreamDescription.Shards[0].ShardId);
        } catch (error) {
          throw new Error(`Kinesis stream ${streamName} not found!`);
        }

        var params = {
          ShardId: shardId,
          ShardIteratorType: 'LATEST',
          StreamName: streamName,
        };

        var shardIter = await kinesis.getShardIterator(params).promise().then(details => details.ShardIterator);

        while (true) {
          var records = await kinesis.getRecords({ ShardIterator: shardIter }).promise();
          shardIter = records.NextShardIterator;
          if (records.Records.length > 0) {
            return JSON.parse(records.Records[0].Data.toString('utf8'));
          }
        }
      }
    }
  }
};

module.exports = aws_funcs;