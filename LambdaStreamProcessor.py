import json
import logging
import time
import boto3
import os
from botocore.exceptions import ClientError

def handler(event, context):
    # Set up logging
    logging.basicConfig(level=logging.DEBUG,format='%(levelname)s: %(asctime)s: %(message)s')
    
    oldRecords = []
	#1. Iterate over each record , in this example we are only focused on REMOVE deleted items by DynamoDB
    try:
        for record in event['Records']:
            #2. Handle event by type
            if record['eventName'] == 'INSERT':
                handle_insert(record)
            elif record['eventName'] == 'MODIFY':
                handle_modify(record)
            elif record['eventName'] == 'REMOVE':
            	if (record['userIdentity']['principalId'] == 'dynamodb.amazonaws.com'):
            		oldRecords.append(record)
        handle_remove(oldRecords)
    except Exception as e:
        logging.error(e)
        return "Error"


def handle_insert(record):
	logging.info("Handling INSERT Event")
	
	#3a. Get newImage content
	newImage = record['dynamodb']['NewImage']
	
	#3b. Parse values
	newReservationId = newImage['ReservationID']['S']

	#3c. log it
	logging.info ('New row added with ReservationID=' + newReservationId)
	logging.info("Done handling INSERT Event")

def handle_modify(record):
	logging.info("Handling MODIFY Event")

	#3a. Parse oldImage and score
	oldImage = record['dynamodb']['OldImage']
	oldReservationDate = oldImage['ReservationDate']['N']
	
	#3b. Parse oldImage
	newImage = record['dynamodb']['NewImage']
	newReservationDate = newImage['ReservationDate']['N']

	#3c. Check for change
	if oldReservationDate != newReservationDate:
		logging.info('Reservation Date Changed  - oldReservationDate=' + str(oldReservationDate) + ', newReservationDate=' + str(newReservationDate))

	logging.info("Done handling MODIFY Event")

def handle_remove(oldRecords):
	logging.info("Handling REMOVE Event")
    
	firehose_client = boto3.client('firehose')
    
	# Assign these values before running the program
	firehose_name = os.environ['firehose_name']
	bucket_arn = os.environ['bucket_arn']
	iam_role_name = os.environ['iam_role_name']
	batch_size = int(os.environ['batch_size'])
	
	oldImages=[]
	#3a. Parse oldImage
	for record in oldRecords:
	    oldImage = record['dynamodb']['OldImage']
	    oldImages.append(oldImage)
	    #3b. Parse values
	    oldReservationId = oldImage['ReservationID']['S']

	    #3c. log it
	    logging.info ('Row removed with ReservationID=' + oldReservationId)
	    
	#3d. Determine the size of the archived data and process in a batch of up to 400 records

	oldImagesSize = len(oldImages)
	result=None

	if oldImagesSize > 0 and oldImagesSize < batch_size :
		batch = [{'Data': json.dumps(oldImages)}]
		try:
			result=firehose_client.put_record_batch(DeliveryStreamName=firehose_name,Records=batch)
		except ClientError as e :
			logging.error(e)
			exit(1)
	elif oldImagesSize > batch_size :
		# Break the list to a batch size of 400 and put record batch
		chunckedOldImagesList = [oldImages[i * batch_size:(i + 1) * batch_size] for i in range((len(oldImages) + batch_size - 1) // batch_size )] 
		for list in chunckedOldImagesList :
			batch = [{'Data': json.dumps(list)}]
			try:
				result=firehose_client.put_record_batch(DeliveryStreamName=firehose_name,Records=batch)
			except ClientError as e :
				logging.error(e)
				exit(1)
		
	# Check for records in the batch did not get processed
	if result :
		num_failures = result['FailedPutCount']
		if num_failures:
			# Resend failed records
			logging.info('Resending {num_failures} failed records')
			rec_index = 0
			for record in result['RequestResponses']:
				if 'ErrorCode' in record:
					# Resend the record
					firehose_client.put_record(DeliveryStreamName=firehose_name,Record=batch[rec_index])
					# Stop if all failed records have been resent
					num_failures -= 1
					if not num_failures:
						break
				rec_index += 1
	logging.info('Data sent to Firehose stream')