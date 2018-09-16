# AWS Lambda functions

## Install AWS-CLI: https://docs.aws.amazon.com/de_de/cli/latest/userguide/cli-install-macos.html

## IoT functions
https://docs.aws.amazon.com/iot/latest/developerguide/iot-sql-functions.html

### Enable logging:
https://docs.aws.amazon.com/de_de/iot/latest/developerguide/cloud-watch-logs.html
```
aws iot set-v2-logging-options \
    --role-arn arn:aws:iam::794552060080:role/AWS_Loggin_Role \
    --default-log-level INFO
```

### aws_lambda
#### Setup Permission

```
aws lambda add-permission --function-name ConvertAWSIotSelectSensorData \
--region eu-west-1 \
--principal iot.amazonaws.com \
--source-arn arn:aws:iot:eu-west-1:794552060080:rule/CONVERT_SAVE_EVENT \
--source-account 794552060080 \
--statement-id IoTSelect_Converter \
--action lambda:InvokeFunction
```

`aws iot get-topic-rule --rule-name CONVERT_SAVE_EVENT`

`$ aws lambda list-functions`

`aws lambda get-function --function-name ConvertAWSIotSelectSensorData`