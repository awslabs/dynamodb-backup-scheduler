#version v14
# Create by : Masudur Rahaman Sayem
from __future__ import print_function
from datetime import date, datetime, timedelta
import json
import boto3
import time
from botocore.exceptions import ClientError

ddbTable = 'test'
backupName = 'Schedule_Backup_V14'
print('Backup started for: ', backupName)
ddb = boto3.client('dynamodb', region_name='us-east-1')

# for deleting old backup. It will search for 2 days old backup and will escape deleting last 2days backup
daysToLookBackup=2
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
		if latestBackupCount>2:
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
		
		

