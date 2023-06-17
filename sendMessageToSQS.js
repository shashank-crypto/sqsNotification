const S3 = require('aws-sdk/clients/s3');
const SQS = require('aws-sdk/clients/sqs')
const xlsx = require('xlsx');

function handler(event, context) {

    console.log('Received event:', JSON.stringify(event));

    bucketName = event.Records[0].s3.bucket.name;
    objectKey = event.Records[0].s3.object.key;

    const sqs = new SQS();
    let s3 = new S3();

    s3.getObject({
        Bucket: bucketName,
        Key: objectKey
    }, function (err, data) {
        if (err) {
            console.log(err, err.stack);
            return {status: 500, message: err.stack};
        } else {
            // convert excel file to json -> data is a excel file
            const file = xlsx.read(data.Body, { type: "buffer" })
            const json = xlsx.utils.sheet_to_json(file.Sheets[file.SheetNames[0]]);
            console.log("jaons",json);
            // send message to SQS in a batch of 10
            for (let i = 0; i < json.length; i += 10) {
                sqs.sendMessageBatch({
                    QueueUrl: process.env.QUEUE_URL,
                    Entries: json.slice(i, i + 10).map((item, index) => {
                        return {
                            Id: index.toString(),
                            MessageBody: JSON.stringify(item)
                        }
                    })
                }, function (err, data) {
                    if (err) {
                        console.log(err, err.stack);
                    } else {
                        console.log(data);
                    }
                });
            }
            return {status: 200, message: 'Success'};
        }
    });
}

module.exports = {handler};