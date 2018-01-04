# Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
from __future__ import print_function
from datetime import date, datetime, timedelta
import json
import boto3
import time
from botocore.exceptions import ClientError
import os
ddbRegion = os.environ['AWS_DEFAULT_REGION']
ddbTable = os.environ['DDBTable']
backupName = 'Schedule_Backup_V21'
print('Backup started for: ', backupName)
ddb = boto3.client('dynamodb', region_name=ddbRegion)

# for deleting old backup. It will search for old backup and will escape deleting last backup days you mentioned in the backup retention
#daysToLookBackup=2
daysToLookBackup= int(os.environ['BackupRetention'])
daysToLookBackupL=daysToLookBackup-1

 
def lambda_handler(event, context):
	try:
		#create backup
		ddb.create_backup(TableName=ddbTable,BackupName = backupName)
		print('Backup has been taken successfully for table:', ddbTable)
		
		#check recent backup
		lowerDate=datetime.now() - timedelta(days=daysToLookBackupL)
		upperDate=datetime.now()
		responseLatest = ddb.list_backups(TableName=ddbTable, TimeRangeLowerBound=datetime(lowerDate.year, lowerDate.month, lowerDate.day), TimeRangeUpperBound=datetime(upperDate.year, upperDate.month, upperDate.day))
		latestBackupCount=len(responseLatest['BackupSummaries'])
		print('Total backup count in recent days:',latestBackupCount)

		deleteupperDate = datetime.now() - timedelta(days=daysToLookBackup)
		print(deleteupperDate)
		# TimeRangeLowerBound is the release of Amazon DynamoDB Backup and Restore - Nov 29, 2017
		response = ddb.list_backups(TableName=ddbTable, TimeRangeLowerBound=datetime(2017, 11, 29), TimeRangeUpperBound=datetime(deleteupperDate.year, deleteupperDate.month, deleteupperDate.day))
		
		#check whether latest backup count is more than two before removing the old backup
		if latestBackupCount>=2:
			if 'LastEvaluatedBackupArn' in response:
				lastEvalBackupArn = response['LastEvaluatedBackupArn']
			else:
				lastEvalBackupArn = ''
			
			while (lastEvalBackupArn != ''):
				for record in response['BackupSummaries']:
					backupArn = record['BackupArn']
					ddb.delete_backup(BackupArn=backupArn)
					print(backupName, 'has deleted this backup:', backupArn)

				response = ddb.list_backups(TableName=ddbTable, TimeRangeLowerBound=datetime(2017, 11, 23), TimeRangeUpperBound=datetime(deleteupperDate.year, deleteupperDate.month, deleteupperDate.day), ExclusiveStartBackupArn=lastEvalBackupArn)
				if 'LastEvaluatedBackupArn' in response:
					lastEvalBackupArn = response['LastEvaluatedBackupArn']
				else:
					lastEvalBackupArn = ''
					print ('the end')
		else:
			print ('Recent backup does not meet the deletion criteria')
		
	except  ClientError as e:
		print(e)

	except ValueError as ve:
		print('error:',ve)
	
	except Exception as ex:
		print(ex)
		
		

