import os
import datetime
import asyncio
import logging
import boto3
import botocore
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey
from anchor.accounts import Universe

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#we should get this from environment variables.
universe_pda = Pubkey.from_string(os.environ['UNIVERSE_ADDRESS'])

# Set async client (devnet_env or mainnet_env)
async_client = AsyncClient(os.environ['MAINNET_ENV'])

# Backup RPC Client
backup_async_client = AsyncClient(os.environ['BACKUP_RPC'])

#SNS
snsClient = boto3.client('sns')

# Set up DB
dynamodb = boto3.resource('dynamodb')
jobsDB = dynamodb.Table("jobs")

def lambda_handler(event, context):
    
    #Datetime Now
    current_datetime = datetime.datetime.now()
    now = int(current_datetime.timestamp())
    
    # default error messages array and valid marker
    valid = True
    
    # Setup loop
    loop = asyncio.get_event_loop()
    
    # Response body
    response_body = {
        'status': 'error',
        'message': []
    } 
    
        
    #POST variables
    if (event == None):
        response_body['message'].append('No post variables found')
        return {
            'statusCode': 200,
            'body': response_body
        }
        
    # validate post data
    if 'wallet' not in event:
        response_body['message'].append("Wallet key not found")
        valid = False
    
    if 'job_type' not in event:
        response_body['message'].append("Task type not found")
        valid = False

    if 'destination' not in event:
        response_body['message'].append("Destination key not found")
        valid = False

    if not valid:
        # print(json.dumps(response_body))
        return {
            'statusCode': 200,
            'body': response_body
        }
    # end validate post variables

    # POST variables
    wallet_pk = event['wallet']
    job_type = event['job_type']
    destination = event['destination']
    
    logger.info("POST (wallet_pk): " + wallet_pk)
    logger.info("POST (job_type): " + job_type)
    logger.info("POST (destination): " + destination)

    # If job type is not withdraw, destination will always be a planet.
    # so we just make sure that the destination planet is part of the universe.
    if job_type != "withdraw":
        # Get Universe data
        # Use asyncio loop run until complete to synchronously "await" an async function
        
        universe = loop.run_until_complete(get_universe())
        if not universe:
            response_body['message'].append("Universe not found")
            # print(json.dumps(error_status))
            return {
                'statusCode': 200,
                'body': response_body
            }
        logger.info("Obtained universe account") 
        
        # Checking planet is in the universe planets list
        if(destination not in universe.p):
            logger.info("Planet not in universe!") 
            response_body['message'].append("Planet not in universe")
            return {
                'statusCode': 200,
                'body': response_body
            }
        logger.info("planet name found in universe")



    # Before adding job make sure we delete any orphaned jobs so there is not a failer. 
    # Checking Job DB - Get job type here!
    job_data = get_job(wallet_pk)
    if job_data:
        logger.info(f"Orphan job was found. Type: {job_data['type']}")
        logger.info("Deleting orphaned job")
        delete_job_result = delete_job(wallet_pk)
        if not delete_job_result:
            response_body['message'].append("Orphaned job found but there was an error deleting from db")
            return {
                'statusCode': 200,
                'body': response_body
            }
        logger.info("Orphan job deleted successfully!") 
        snsMessage = "Orphaned job type " + job_type +  " has been deleted for " + wallet_pk
        send_sns(snsMessage)


    ############## ADD JOB TO DB ############
    logger.info("Submitting to DB...")

    try:
        jobsDB.put_item(
            Item={
                "wallet": wallet_pk,
                "type": job_type,
                "destination": destination,
                "created": now,
                "completed": 0
            },
            ConditionExpression='attribute_not_exists(wallet)',
        )
    except botocore.exceptions.ClientError as err:
        logger.error(
            "Couldn't add job. Error: %s: %s",
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        response_body['message'].append("Record error")
        return {
            'statusCode': 200,
            'body': response_body
        }
        

    logger.info("Job created successfully")    
    logger.info("Done") 
    
    return {
        'statusCode' : 200,
        'body': {
            'status':'success',
            'message':'Job created successfully'
        }
    }
    
    
# fetch universe account
async def get_universe():
    try:
        acc = await Universe.fetch(async_client, universe_pda)
        logger.info("Helius RPC API succeeded")
    except:
        logger.info("Helius RPC Error, trying mainnet api..")
        try:
            acc = await Universe.fetch(backup_async_client,universe_pda)
            logger.info("Main RPC API succeeded")
        except:
            logger.info("Both Helius and Main RPC API failed")
            acc = False
    return acc


def get_job(wallet):
        """
        Gets job data from table 

        :param wallet: Wallet string of user.
        :return: The deposit data row
        """
        try:
            response = jobsDB.get_item(Key={"wallet": wallet})
        except botocore.exceptions.ClientError as err:
            logger.error(
                "Couldn't get deposit. Error: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            if "Item" in response:
                return response["Item"]
            else:
                return False
            
def delete_job(wallet):
        """
        Delete job for wallet
        :param wallet: updating wallet deposit (Key)
        :return: Boolean of success or failure.
        """
        
        try:
            jobsDB.delete_item(Key={"wallet": wallet})
        except botocore.exceptions.ClientError as err:
            logger.error(
                "Couldn't delete deposit! Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            return False
        else:
            return True
        

def send_sns(snsMessage):
    snsClient.publish(TopicArn='arn:aws:sns:us-west-1:058264465436:TaskComplete',Message=snsMessage)
    print("Message published")
    return