console.log('Loading function');

const uuidV4 = require('uuid/v4');

const doc = require('dynamodb-doc');

// https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html
const dynamo = new doc.DynamoDB();


exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));

    /* Payload
    {   "msgid": "guid",
        "batt" : { "N" : "-1" },
        "humidity" : { "N" : "38.3" },
        "light_intensity" : { "N" : "66" },
        "messageIndex" : { "N" : "233" },
        "msgid" : { "S" : "114d71fe.24a12e" },
        "node" : { "S" : "B9FC4586" },
        "pressure" : { "N" : "96131.94" },
        "primary_temperature" : { "N" : "28" },
        "secondary_temperature" : { "N" : "28.38" },
        "timeStamp" : { "N" : "1535819532439" }  }
    */
    /*
    Sample for batch write: https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#batchWriteItem-property
    
     var params = {
      RequestItems: {
       "Music": [
           {
          PutRequest: {
           Item: {
            "AlbumTitle": {
              S: "Somewhat Famous"
             }, 
            "Artist": {
              S: "No One You Know"
             }, 
            "SongTitle": {
              S: "Call Me Today"
             }
           }
          }
         }, 
           {
          PutRequest: {
           Item: {
            "AlbumTitle": {
              S: "Songs About Life"
             }, 
            "Artist": {
              S: "Acme Band"
             }, 
            "SongTitle": {
              S: "Happy Day"
             }
           }
          }
         }, 
           {
          PutRequest: {
           Item: {
            "AlbumTitle": {
              S: "Blue Sky Blues"
             }, 
            "Artist": {
              S: "No One You Know"
             }, 
            "SongTitle": {
              S: "Scared of My Shadow"
             }
           }
          }
         }
        ]
      }
     };
     dynamodb.batchWriteItem(params, function(err, data) {
       if (err) console.log(err, err.stack); // an error occurred
       else     console.log(data);           // successful response
       data = {
       }
    });
    */


    const rawToObject = (entry) => {
        const timeStamp = parseInt(entry._id);
        const buf = new Buffer(entry.data_raw, 'base64)');
        const values = buf.toString();
        //console.log(JSON.stringify(values));
        const seperated = values.split('|')
            .filter(v => Boolean(v)); // filter out dummy values

        const toValue = (value) => {
            const intValue = parseInt(value);
            if (isNaN(intValue) || value === '-1') {
                return -1;
            }
            return intValue / 100;
        };

        const returnValue = {
            msgid: uuidV4(),
            timeStamp: timeStamp,
            node: seperated[0],
            messageIndex: parseInt(seperated[1]),
            primary_temperature: toValue(seperated[2]),
            humidity: toValue(seperated[3]),
            light_intensity: parseInt(seperated[4]),
            batt: toValue(seperated[5]),
            secondary_temperature: toValue(seperated[6]),
            pressure: toValue(seperated[7]),
        };
        return returnValue
    }

    const source_table_name = 'TestIoT';
    const target_table_name = 'IoTData';

    const batchInsert = (items, callbackInsert) => {
        console.log('Mapping data');
        const jsonDataArray = items.map((item) => {
            try{
                if (item.data) { // already json
                    return item.data;
                }
                return rawToObject(item)
            } catch(e) {
                console.log('Failed to convert item:', JSON.stringify(item));
                callback(e);
            }
        });

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

        dynamo.batchWrite(batchRequest, function (err, data) {
            if (err) {
                console.log(err, err.stack); // an error occurred
                callbackInsert(err);
            }
            else {
                console.log(data);           // successful response
                callbackInsert(null, data);
            }
        });
    };

    const scanDone = (err, res) => {
        if (err) {
            console.error(err);
            callback(err);
            return;
        }

        console.log('Read:', res.Count);

        batchInsert(res.Items, (err, data) => {
            if (err) {
                console.error(err);
                callback(err);
                return;
            } 

            if (!Boolean(res.LastEvaluatedKey)) {
                callback(null, data);
                console.log('Done insert');
            }
        });

        if (res.LastEvaluatedKey) {
            scanSourceTable(source_table_name, res.LastEvaluatedKey, scanDone);
        }
        else {
            console.log("Done scan");
        }
    };


    scanSourceTable(source_table_name, null, scanDone);

};

const scanSourceTable = (tableName, exclusiveStartKey, scanCallback) => {

    const scanObject = {
        TableName: tableName,
    };
    if (exclusiveStartKey) {
        scanObject["ExclusiveStartKey"] = exclusiveStartKey;
    }
    console.log(`Scanning "${tableName}"`, exclusiveStartKey ? `with key: ${exclusiveStartKey}` : null);
    dynamo.scan(scanObject, scanCallback);
};
