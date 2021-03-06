console.log('Loading function');

// Load the AWS SDK for Node.js
var AWS = require('aws-sdk');
// Set the region 
AWS.config.update({ region: 'eu-west-1' });

// Create DynamoDB service object
var ddb = new AWS.DynamoDB({ apiVersion: '2012-08-10' });

exports.handler = (event, context, callback) => {

    const table_name = 'EnvironmentSources';

    var params = {
        TableName: table_name,
        ProjectionExpression: '#attr_source, display_name, topic',
        ExpressionAttributeNames: {
            "#attr_source": "source"
        }
    };
    const dataContext = {
        path: buildBaseURL(event)
    }

    const done = (err, res) => {
        let response;
        if (err) {
            response = errorHandling(err);
        }
        else {
            response = {
                statusCode: '200',
                body: mapResponse(dataContext, res),
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true',
                }
            }
        }

        callback(null, response);
    };

    console.log('scan parameters:', JSON.stringify(params));
    switch (event.httpMethod) {
        case 'GET':
            ddb.scan(params, done);
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

const buildBaseURL = function (event) {
    if (!event.headers || !("X-Forwarded-Proto" in event.headers)) {
        return event.requestContext.path;
    }
    return `${event.headers["X-Forwarded-Proto"]}://${event.headers.Host}${event.requestContext.path}`;
}

const mapResponse = function (context, res) {
    const data = res.Items
        .map(convertDynamoRepresentation)
        .map(d => addRelations(context, d));
    const response = {
        data: data,
        count: data.length
    };
    return JSON.stringify(response);
};

const addRelations = function (context, item) {
    item.relations = {
        nodes: {
            href: `${context.path}/${item.source}/nodes`
        }
    }

    return item;
}

const convertDynamoRepresentation = function (obj) {
    return Object.keys(obj).reduce((result, key) => {
        const value = obj[key];
        if (typeof value === 'object') {
            const subKeys = Object.keys(value);
            if (subKeys.length !== 1) {
                return convertDynamoRepresentation(value);
            }
            switch (subKeys[0]) {
                case 'S':
                    result[key] = value.S;
                    break;
                case 'N':
                    result[key] = Number.parseFloat(value.N);
                    break;
                case 'M':
                    result[key] = convertDynamoRepresentation(value.M);
                    break;
                default:
                    throw `Not supported type: ${JSON.stringify(value)}`;
            }
        }
        return result;
    }, {})
}


/* event object
{
    "resource": "/sources",
    "path": "/sources",
    "httpMethod": "GET",
    "headers": null,
    "multiValueHeaders": null,
    "queryStringParameters": null,
    "multiValueQueryStringParameters": null,
    "pathParameters": null,
    "stageVariables": null,
    "requestContext": {
        "path": "/sources",
        "accountId": "794552060080",
        "resourceId": "v2bjxw",
        "stage": "test-invoke-stage",
        "requestId": "7cd68172-afc9-11e8-8a48-8fc51713c1b3",
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
        "resourcePath": "/sources",
        "httpMethod": "GET",
        "extendedRequestId": "MqmEIG1_DoEFSmQ=",
        "apiId": "zjpehz8xi5"
    },
    "body": null,
    "isBase64Encoded": false
}
*/