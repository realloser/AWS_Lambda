console.log('Loading function');

// https://www.npmjs.com/package/dynamodb-doc
var AWS = require('aws-sdk');
var DOC = require('dynamodb-doc');

AWS.config.update({ region: 'eu-west-1' });

var docClient = new DOC.DynamoDB();

exports.handler = (event, context, callback) => {

    const doneConsole = (err, res) => {
        if (err) {
            console.error(err);
        }

        console.log(JSON.stringify(res));
        callback('failed');
    }

    const queryContext = {
        tableName: 'EnvironmentData',
        node: event.pathParameters['node_id'],
        startTime: new Date().getTime() - 5 * 60 * 60 * 1000,
    };

    if ('proxy' in event.pathParameters) {
        const proxyArray = event.pathParameters['proxy'].split('/') || [];

        queryContext.top = proxyArray[0].toLowerCase() === 'top';
    }

    // const done = doneConsole;
    const done = (err, res) => {
        console.timeEnd('query');

        let response;
        if (err) {
            response = errorHandling(err);
        }
        else {
            response = {
                statusCode: '200',
                body: mapResponse(queryContext, res),
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true',
                }
            }
        }

        callback(null, response);
    };

    switch (event.httpMethod) {
        case 'GET':
            queryTable(queryContext, done);
            break;
        default:
            done(new Error(`Unsupported method "${event.httpMethod}"`));
    }
};

const errorHandling = (err) => {

    switch (err.code) {
        case 'ResourceNotFoundException':
            return {
                statusCode: 404,
                body: 'No data found',
                headers: {
                    'Content-Type': 'application/json',
                }
            };
        default:
            console.log('Unhandled exception', JSON.stringify(err));
            return {
                statusCode: 500,
                body: `Server error, unhandled exception. Request-ID: ${err.requestId}`,
                headers: {
                    'Content-Type': 'application/json',
                }
            };
    }
};

const queryTable = function (context, callback) {

    // conditions [IN, NULL, BETWEEN, LT, NOT_CONTAINS, EQ, GT, NOT_NULL, NE, LE, BEGINS_WITH, GE, CONTAINS]

    var params = {
        TableName: context.tableName,
        ScanIndexForward: false,
        KeyConditions: [docClient.Condition('node', 'EQ', context.node)],
    };

    addAttributeFilter(context, params);

    if (context.top) {
        params.Limit = 1;
    }
    else {
        params.KeyConditions.push(context.endTime
            ? docClient.Condition('timeStamp', 'BETWEEN', `${context.endTime}`, `${context.startTime}`)
            : docClient.Condition('timeStamp', 'GE', `${context.startTime}`));
    }

    console.log('query params', JSON.stringify(params));
    console.time('query')
    docClient.query(params, callback);
}

const addAttributeFilter = function (context, params) {
    // specify a default attribute filter
    let availableAttributes = [
        // "node", // already part of the main body response
        //"timeStamp", // keyword
        "primary_temperature",
        "secondary_temperature",
        "batt",
        // "messageIndex",
        // "msgid",
        "humidity",
        "light_intensity",
        "pressure"];

    let keyWordAttr = ['timeStamp'];
    if (keyWordAttr) {
        params.ExpressionAttributeNames = keyWordAttr.reduce((obj, attr, i) => {
                const key = `#attr_${attr}`;
                obj[key] = `${attr}`;
                availableAttributes.push(key)
                return obj;
            }, {});
    }

    params.ProjectionExpression = availableAttributes.map((a) => `payload.${a}`).join(', ');
};

const mapResponse = function (context, res) {
    const data = res.Items.map(item => item.payload);
    const response = {
        node: context.node,
        data: context.top ? data[0] : data,
    };
    !context.top && (response.count = data.length);
    return JSON.stringify(response);
};


/* dynamoDB response
{
  "Items": [
    {
      "payload": {
        "timeStamp": 1535914798040,
        "primary_temperature": 28.2,
        "node": "B9FC4586",
        "batt": -1,
        "secondary_temperature": 29,
        "messageIndex": 1027,
        "msgid": "e1061554.33ed48",
        "humidity": 36.9,
        "light_intensity": 0,
        "pressure": 95706.81
      },
      "node": "B9FC4586",
      "timeStamp": "1535914798040"
    },
    {
      "payload": {
        "timeStamp": 1535914918022,
        "primary_temperature": 28.2,
        "node": "B9FC4586",
        "batt": -1,
        "secondary_temperature": 28.88,
        "messageIndex": 1028,
        "msgid": "71bcd735.08c278",
        "humidity": 36.6,
        "light_intensity": 0,
        "pressure": 95702.69
      },
      "node": "B9FC4586",
      "timeStamp": "1535914918022"
    }
  ],
  "Count": 2,
  "ScannedCount": 2,
  "LastEvaluatedKey": {
    "node": "B9FC4586",
    "timeStamp": "1535914918022"
  }
}
*/

/* event example
{
    "resource": "/sources/{source_id}/nodes/{node_id}",
    "path": "/sources/thesourceid/nodes/thenodeid",
    "httpMethod": "GET",
    "headers": null,
    "queryStringParameters": {
        "query1": "aba",
        "query2": "baba"
    },
    "pathParameters": {
        "source_id": "thesourceid",
        "node_id": "B9FC4586"
    },
    "stageVariables": null,
    "requestContext": {
        "path": "/sources/{source_id}/nodes/{node_id}",
        "accountId": "794552060080",
        "resourceId": "533zqp",
        "stage": "test-invoke-stage",
        "requestId": "984b9077-af9f-11e8-a8b3-9314f36fb329",
        "identity": {
            "cognitoIdentityPoolId": null,
            "cognitoIdentityId": null,
            "apiKey": "test-invoke-api-key",
            "cognitoAuthenticationType": null,
            "userArn": "arn:aws:iam::794552060080:root",
            "apiKeyId": "test-invoke-api-key-id",
            "userAgent": "aws-internal/3 aws-sdk-java/1.11.347 Linux/4.9.110-0.1.ac.201.71.329.metal1.x86_64 Java_HotSpot(TM)_64-Bit_Server_VM/25.172-b31 java/1.8.0_172",
            "accountId": "794552060080",
            "caller": "794552060080",
            "sourceIp": "test-invoke-source-ip",
            "accessKey": "ASIA3R7X6JCYEDYT73SI",
            "cognitoAuthenticationProvider": null,
            "user": "794552060080"
        },
        "resourcePath": "/sources/{source_id}/nodes/{node_id}",
        "httpMethod": "GET",
        "extendedRequestId": "Mp6IwFgnDoEFW1w=",
        "apiId": "zjpehz8xi5"
    },
    "body": null,
    "isBase64Encoded": false
}

*/

/* Request with {proxy+}

{
  "resource": "/sources/{source_id}/nodes/{node_id}",
  "path": "/sources/thesourceid/nodes/thenodeid",
  "httpMethod": "GET",
  "headers": null,
  "queryStringParameters": {
    "query1": "aba",
    "query2": "baba"
  },
  "pathParameters": {
    "source_id": "thesourceid",
    "node_id": "B9FC4586"
  },
  "stageVariables": null,
  "requestContext": {
    "path": "/sources/{source_id}/nodes/{node_id}",
    "accountId": "794552060080",
    "resourceId": "533zqp",
    "stage": "test-invoke-stage",
    "requestId": "984b9077-af9f-11e8-a8b3-9314f36fb329",
    "identity": {
      "cognitoIdentityPoolId": null,
      "cognitoIdentityId": null,
      "apiKey": "test-invoke-api-key",
      "cognitoAuthenticationType": null,
      "userArn": "arn:aws:iam::794552060080:root",
      "apiKeyId": "test-invoke-api-key-id",
      "userAgent": "aws-internal/3 aws-sdk-java/1.11.347 Linux/4.9.110-0.1.ac.201.71.329.metal1.x86_64 Java_HotSpot(TM)_64-Bit_Server_VM/25.172-b31 java/1.8.0_172",
      "accountId": "794552060080",
      "caller": "794552060080",
      "sourceIp": "test-invoke-source-ip",
      "accessKey": "ASIA3R7X6JCYEDYT73SI",
      "cognitoAuthenticationProvider": null,
      "user": "794552060080"
    },
    "resourcePath": "/sources/{source_id}/nodes/{node_id}",
    "httpMethod": "GET",
    "extendedRequestId": "Mp6IwFgnDoEFW1w=",
    "apiId": "zjpehz8xi5"
  },
  "body": null,
  "isBase64Encoded": false
}

{
  "resource": "/sources/{source_id}/nodes/{node_id}/{proxy+}",
  "path": "/sources//nodes/B9FC4586/top/huu/haaa",
  "httpMethod": "GET",
  "headers": null,
  "queryStringParameters": null,
  "pathParameters": {
    "proxy": "top/huu/haaa",
    "source_id": "",
    "node_id": "B9FC4586"
  },
...
}

*/