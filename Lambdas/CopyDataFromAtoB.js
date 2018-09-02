console.log('Loading function');

const uuidV4 = require('uuid/v4');

// // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html
// Load the AWS SDK for Node.js
var AWS = require('aws-sdk');
// Set the region 
AWS.config.update({ region: 'eu-west-1' });

// Create DynamoDB service object
var ddb = new AWS.DynamoDB({ apiVersion: '2012-08-10' });



exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));

    const rawToObject = (entry) => {
        const timeStamp = parseInt(entry._id.S);
        if (isNaN(timeStamp)) {
            return null;
        }

        const baseData = entry.data_raw.B;

        if (!Boolean(baseData)) {
            return null;
        }

        const buf = new Buffer(baseData, 'base64)');
        const values = buf.toString();
        const separator = values.split('|')
            .filter(v => Boolean(v)); // filter out dummy values

        if (!Boolean(separator[0])) { // no node
            return null;
        }

        const toValue = (value) => {
            const intValue = parseInt(value);
            if (isNaN(intValue) || value === '-1') {
                return '-1';
            }
            return `${intValue / 100}`;
        };

        const toInt = (value) => {
            const intValue = parseInt(value);
            if (isNaN(intValue) || value === '-1') {
                return '-1';
            }
            return `${intValue}`;
        };

        const uuid = `${uuidV4()}`;
        const payload = {
            msgid: { S: uuid },
            timeStamp: { N: `${timeStamp}` },
            node: { S: separator[0] },
            messageIndex: { N: toInt(separator[1]) },
            primary_temperature: { N: toValue(separator[2]) },
            humidity: { N: toValue(separator[3]) },
            light_intensity: { N: toInt(separator[4]) },
            batt: { N: toValue(separator[5]) },
            secondary_temperature: { N: toValue(separator[6]) },
            pressure: { N: toValue(separator[7]) },
        };

        const returnValue = {
            "_id": { S: payload.msgid.S },
            node: { S: payload.node.S },
            timeStamp: { S: payload.timeStamp.N },
            payload: { M: payload }
        }
        return returnValue
    }

    const source_table_name = 'TestIoT';
    const target_table_name = 'IoTData'; //'EnvironmentData';

    const batchInsert = (items, callbackInsert) => {
        console.log('Mapping data');
        const jsonDataArray = items.map((item) => {
            try {
                if (item.data || !Boolean(item.data_raw)) { // ignore existing json or empty data
                    return null;
                }
                const obj = rawToObject(item)
                // console.log(obj);
                return obj;
            } catch (e) {
                console.log('Failed to convert item:', JSON.stringify(item));
                callback(e);
                return null;
            }
        }).filter(i => Boolean(i));

        const batchRequest = {
            RequestItems: {
                [target_table_name]: jsonDataArray.map((json) => (
                    {
                        PutRequest: {
                            Item: json
                        }
                    }))
            }
        };

        console.log(JSON.stringify(batchRequest));
        throw "stop"; 
        console.log('Write data:', jsonDataArray.length);
        if (jsonDataArray.length) {

            
            // Promise.all(jsonDataArray.map(d => {
            //     const putItem = {
            //         TableName: target_table_name,
            //         Item: d
            //     }
            //     ddb.putItem(putItem, function (err, data) {
            //         try {
            //             if (err) {
            //                 console.log('Failed to insert:', d);
            //                 console.error('putItem failed:', err); // an error occurred
            //                 // callbackInsert(err);
            //             }
            //             else {
            //                 // console.log(data);           // successful response
            //                 // callbackInsert(null, data);
            //             }
            //         }
            //         catch (e) {
            //             console.log('Failed to insert:', d);
            //             console.error(e);
            //         }
            //     });
            // }));
            
            // does always fail with ValidationException, empty data. Hard to figure the problem.
            ddb.batchWriteItem(batchRequest, function (err, data) {
                if (err) {
                    console.error('BatchWriteFailed:', err); // an error occurred
                    callbackInsert(err);
                }
                else {
                    console.log(data);           // successful response
                    callbackInsert(null, data);
                }
            });
        }

        callbackInsert(null, 'done');
    };

    const scanDone = (err, res) => {
        if (err) {
            console.error(err);
            callback(err);
            return;
        }

        console.log('Read:', res.Count);

        // console.log(JSON.stringify(res));

        console.log('LastEvaluatedKey', JSON.stringify(res.LastEvaluatedKey));

        batchInsert(res.Items, (err, data) => {
            if (err) {
                console.error(err);
                callback(err);
                return;
            }

            if (!Boolean(res.LastEvaluatedKey)) {
                console.log('Done insert');
                callback(null, data);
                
            }
        });

        
        if (res.LastEvaluatedKey) {
            //scanSourceTable(source_table_name, res.LastEvaluatedKey, scanDone);
        }
        else {
            console.log("Done scan");
        }
    };
    const exclusiveStartKey = {
        "_id": {
            "S": "1535042598491"
        }
    };

    scanSourceTable(source_table_name, exclusiveStartKey, scanDone);
};

const scanSourceTable = (tableName, exclusiveStartKey, scanCallback) => {

    const scanObject = {
        TableName: tableName,
        Limit: 50
    };
    if (exclusiveStartKey) {
        scanObject["ExclusiveStartKey"] = exclusiveStartKey;
    }
    console.log(`Scanning "${tableName}"`, exclusiveStartKey ? `with key: ${JSON.stringify(exclusiveStartKey)}` : null);
    ddb.scan(scanObject, scanCallback);
};
