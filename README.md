# Archive items to S3 using DynamoDB TLL

Automatically archive items to Amazon S3 using Amazon DynamoDB TTL, AWS Lambda, and Amazon Kinesis Data Firehose

This code is related to the following APG and it is an addition/supplemnet to the instruction in the APG to implement the solution: https://apg-library.amazonaws.com/content-viewer/author/9dbc833f-cf3c-4574-8f09-d0b81134fe41

This pattern provides steps to remove older data from an Amazon DynamoDB table and archive it to Amazon S3 bucket without having to manage a fleet of servers. 

This pattern uses Amazon DynamoDB Time to Live (TTL) to automatically delete old items and Amazon DynamoDB Streams to capture the TTL-expired items. It then connects DynamoDB Streams to AWS Lambda, which runs the code without provisioning or managing any servers. 

When new items are added to the DynamoDB stream, the Lambda function is initiated and writes the data to an Amazon Kinesis Data Firehose delivery stream. Kinesis Data Firehose provides a simple, fully managed solution to load the data as an archive into Amazon Simple Storage Service (Amazon S3).

DynamoDB is often used to store time series data, such as webpage click-stream data or Internet of Things (IoT) data from sensors and connected devices. Rather than deleting less frequently accessed items, many customers want to archive them for auditing purposes. TTL simplifies this archiving by automatically deleting items based on the timestamp attribute. 

Items deleted by TTL can be identified in DynamoDB Streams, which captures a time-ordered sequence of item-level modifications and stores them in a log for up to 24 hours. This data can be consumed by a Lambda function and archived in Amazon S3 bucket to reduce the storage cost. To further reduce the costs, Amazon S3 life-cycle rules can be created to automatically transition the data (as soon as it gets created) to lowest-cost storage classes such as S3 Glacier Instant Retrieval or S3 Glaciar Flexible Retrieval or S3 Glaciar Deep Archive for long-term storage
