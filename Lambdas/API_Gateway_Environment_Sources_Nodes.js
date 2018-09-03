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

    const done = (err, res) => {
        let response;
        if (err) {
            response = errorHandling(err);
        }
        else {
            response = {
                statusCode: '200',
                body: mapResponse(res),
                headers: {
                    'Content-Type': 'application/json',
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


const mapResponse = function (res) {
    const source = res.Items[0];
    const data = source.nodes;
    const response = {
        source: source.source,
        displayName: source.display_name,
        data: data,
        count: data.length
    };
    return JSON.stringify(convertDynamoRepresentation(response));
};

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