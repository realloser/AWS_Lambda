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
        TableName: table_name
    };

    const dataContext = {
        path: buildBaseURL(event)
    }

    const done = (err, res) => {
        let response;
        if (err) {
            console.log('event:', JSON.stringify(event));
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

    switch (event.httpMethod) {
        case 'GET':
            ddb.scan(params, done);
            break;
        default:
            done(new Error(`Unsupported method "${event.httpMethod}"`));
    }

};

const buildBaseURL = function (event) {
    if (!event.headers || !("X-Forwarded-Proto" in event.headers)) {
        return event.requestContext.path;
    }
    return `${event.headers["X-Forwarded-Proto"]}://${event.headers.Host}${event.requestContext.path}`;
}

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

const mapResponse = function (context, res) {
    const sources = res.Items
        .map(convertDynamoRepresentation);

    const dataCollection = sources.map((s) => {
        const nodes = Object.keys(s.nodes)
            .map(k => Object.assign({ node: k }, k[k]))
            .map(n => addRelations(context, n));
        return {
            source: s.source,
            displayName: s.display_name,
            nodes: nodes
        };
    });
    const response = {
        data: dataCollection,
        count: dataCollection.length
    };
    return JSON.stringify(response);
};


const addRelations = function (context, item) {
    item.relations = {
        data: {
            href: `${context.path}/${item.node}`
        },
        top: {
            href: `${context.path}/${item.node}/top`
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
    "resource": "/sources/{source_id}/nodes",
    "path": "/sources/adasdf/nodes",
    "httpMethod": "GET",
    "headers": null,
    "multiValueHeaders": null,
    "queryStringParameters": null,
    "multiValueQueryStringParameters": null,
    "pathParameters": {
        "source_id": "adasdf"
    },
    "stageVariables": null,
    "requestContext": {
        "path": "/sources/{source_id}/nodes",
        "accountId": "794552060080",
        "resourceId": "4cqbn3",
        "stage": "test-invoke-stage",
        "requestId": "0f727a71-afcc-11e8-88b0-9713fe0f789a",
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
        "resourcePath": "/sources/{source_id}/nodes",
        "httpMethod": "GET",
        "extendedRequestId": "MqowyEoujoEFv-g=",
        "apiId": "zjpehz8xi5"
    },
    "body": null,
    "isBase64Encoded": false
}

*/

/* API Gateway event

{
    "resource": "/sources/{source_id}/nodes",
    "path": "/sources/aa/nodes/",
    "httpMethod": "GET",
    "headers": {
        "Accept": "",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en,de-CH;q=0.9,de;q=0.8,en-NZ;q=0.7",
        "Cache-Control": "no-cache",
        "Host": "zjpehz8xi5.execute-api.eu-west-1.amazonaws.com",
        "Postman-Token": "d035f7cb-ba81-411c-f36d-cfcae36ac631",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
        "X-Amzn-Trace-Id": "Root=1-5b8ecdf9-78631c94f3426886d10b139b",
        "X-Forwarded-For": "5.149.17.131",
        "X-Forwarded-Port": "443",
        "X-Forwarded-Proto": "https"
    },
    "queryStringParameters": null,
    "pathParameters": {
        "source_id": "aa"
    },
    "stageVariables": null,
    "requestContext": {
        "resourceId": "4cqbn3",
        "resourcePath": "/sources/{source_id}/nodes",
        "httpMethod": "GET",
        "extendedRequestId": "MtUfCHyojoEF95g=",
        "requestTime": "04/Sep/2018:18:24:57 +0000",
        "path": "/V1/sources/aa/nodes/",
        "accountId": "794552060080",
        "protocol": "HTTP/1.1",
        "stage": "V1",
        "requestTimeEpoch": 1536085497862,
        "requestId": "d3f3aaef-b06f-11e8-b5d8-a990f62a4c86",
        "identity": {
            "cognitoIdentityPoolId": null,
            "accountId": null,
            "cognitoIdentityId": null,
            "caller": null,
            "sourceIp": "5.149.17.131",
            "accessKey": null,
            "cognitoAuthenticationType": null,
            "cognitoAuthenticationProvider": null,
            "userArn": null,
            "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
            "user": null
        },
        "apiId": "zjpehz8xi5"
    },
    "body": null,
    "isBase64Encoded": false
}

*/