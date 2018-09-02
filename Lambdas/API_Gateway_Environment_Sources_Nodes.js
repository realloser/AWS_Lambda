console.log('Loading function');

const doc = require('dynamodb-doc');

const dynamo = new doc.DynamoDB();


/**
 * Demonstrates a simple HTTP endpoint using API Gateway. You have full
 * access to the request and response payload, including headers and
 * status code.
 *
 * To scan a DynamoDB table, make a GET request with the TableName as a
 * query string parameter. To put, update, or delete an item, make a POST,
 * PUT, or DELETE request respectively, passing in the payload to the
 * DynamoDB API as a JSON body.
 */
exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));

/*
{
      "data_raw": {
        "type": "Buffer",
        "data": [
          70,
          65,
          50,
          52,
          67,
          50,
          65,
          51,
          124,
          50,
          51
        ]
      },
      "_id": "1535547253418"
    }
*/

    const handleScanResult = (err, res) => {
        if (err) {
            done(err);
            return;
        }
        const mapped = res.Items && res.Items.map(rawToObject) || [];
        doneConsole(null, {data: mapped});
        
    };
    
    const rawToObject = (entry) => {
        const timeStamp = parseInt(entry._id);
        const buf = new Buffer(entry.data_raw, 'base64)');
        const values = buf.toString();
        console.log(JSON.stringify(values));
        const seperated = values.split('|')
            .filter(v => Boolean(v)); // filter out dummy values
        
        const toValue = (value) => {
            const intValue = parseInt(value);
            if (isNaN(intValue) || -1) {
                return -1;
            }
            return intValue/100;  
        };
        
        const returnValue = {
            timeStamp: timeStamp,
            node: seperated[0],
            messageIndex: parseInt(seperated[1]),
            primary_temperature: toValue(seperated[2]),
            humidity: toValue(seperated[3]),
            light_intensity: seperated[4],
            batt: toValue(seperated[5]),
            secondary_temperature: toValue(seperated[6]),
            pressure: toValue(seperated[7]),
        };
        return returnValue
    }
    
    const doneConsole = (err, res) => console.log(JSON.stringify(res));

    
    const done = (err, res) => callback(null, {
        statusCode: err ? '400' : '200',
        body: err ? err.message : JSON.stringify(res),
        headers: {
            'Content-Type': 'application/json',
        },
    });
    
    const table_name = 'TestIoT';
    // const timeStamp = new Date(Date.now() - (24 * 60 * 60 * 1000));
    
    const scanObject = {    
        TableName: table_name,
        Limit: 5
        // FilterExpression: "_id < :ts",
        // ExpressionAttributeValues: {
        //     ":ts": timeStamp,
        // }
    };
    dynamo.scan(scanObject, handleScanResult);

};
