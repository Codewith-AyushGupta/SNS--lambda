// this lambda is responsible for getting response from S3 bucket and sending the notification to SNS
const AWS = require('aws-sdk');
const sns = new AWS.SNS();
const topicArn = "arn:aws:sns:us-east-1:975050164323:ShopifiySNS";

async function sendSns(message, subject) {
    try {
        const params = {
            Message: message,
            Subject: subject,
            TopicArn: topicArn
        };
        
        const result = await sns.publish(params).promise();
        
        if (result && result.ResponseMetadata && result.ResponseMetadata.HTTPStatusCode === 200) {
            console.log(result);
            console.log("Notification sent successfully..!!!");
            return true;
        }
    } catch (e) {
        console.error("Error occurred while publishing notifications: ", e);
        return false;
    }
}

exports.handelData = async (event) => {
    console.log("Event collected is ", JSON.stringify(event));

    for (const record of event.Records) {
        const s3Bucket = record.s3.bucket.name;
        console.log("Bucket name is ", s3Bucket);
        
        const s3Key = record.s3.object.key;
        console.log("Bucket key name is ", s3Key);
        
        const fromPath = `s3://${s3Bucket}/${s3Key}`;
        console.log("From path ", fromPath);
        
        const message = `The file is uploaded at S3 bucket path ${fromPath}`;
        const subject = "Processes completion Notification";
        
        const snsResult = await sendSns(message, subject);
        
        if (snsResult) {
            console.log("Notification Sent..");
            return snsResult;
        } else {
            return false;
        }
    }
};
