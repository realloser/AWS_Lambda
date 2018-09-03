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
        console.timeEnd('query');

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

    ddb.scan(params, done);
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
                throw `not expected value: ${JSON.stringify(value)}`;
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
                    throw 'Not supported type: ${JSON.stringify(value)}';
            }
        }
        return result;
    }, {})
}