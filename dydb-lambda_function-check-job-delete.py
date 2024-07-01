import json
import logging
import boto3
import botocore
from solders.pubkey import Pubkey

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
depositsDB = dynamodb.Table("deposits")
jobsDB = dynamodb.Table("jobs")


# Pass wallet, hop transaction, to planet name 
def lambda_handler(event, context):
        
    # Response body
    response_body = {
        'status': 'error',
        'message': []
    } 
        

    # Validate post variables
    if 'wallet' not in event:
        response_body['message'].append("Wallet public key not found")
        logger.error('Wallet not found. Post data validation error')
        return {
            'statusCode': 200,
            'body': response_body
        }
        
    
    # validate wallet variable
    wallet = event['wallet'] 
    wallet_pk = Pubkey.from_string(wallet)
    if not wallet_pk.is_on_curve:
        response_body['message'].append("Wallet key not valid")
        return {
            'statusCode': 200,
            'body': response_body
        }
    logger.info("Wallet key valid: " + wallet)
    
    # Get deposit - (To get the from planet and deposit lamports)
    deposit_data = get_deposit(wallet)
    if not deposit_data:
        logger.info("Deposit data was not found. Ending.")
        response_body['message'].append("Deposit data for wallet address not found")
        return {
            'statusCode': 200,
            'body': response_body
        }
    logger.info("Deposit data found for wallet")
    logger.info("Now checking job found for wallet")
    

    # Checking Job DB - Get job type here!
    job_data = get_job(wallet)
    if not job_data:
        response_body['message'].append("Job not found for wallet address")
        return {
            'statusCode': 200,
            'body': response_body
        }
    
    job_type = job_data['type']

    # If job is not completed return immediately with job still pending message.
    job_completed = job_data['completed']
    if not job_completed:
        logger.info("Job found but still not completed")
        response_body['status'] = 'pending'
        response_body['message'] = 'Job still pending'
        return {
            'statusCode': 200,
            'body': response_body
        }

    # Job is completed! Woot! So now we delete the job and send back and completed message. 
    logger.info("Job is now completed!") 
    
    # Delete job
    delete_job_result = delete_job(wallet)
    if not delete_job_result:
        response_body['message'].append("Job was completed but there was an error deleting job from db")
        return {
            'statusCode': 200,
            'body': response_body
        }

    logger.info("Job deleted successfully!")    

    # If the job type is withdraw, we delete the deposit here!
    if job_type == "withdraw":
        delete_result = delete_deposit(wallet)
        if not delete_result:
            print("There was an error deleting deposit from db")
            return
        logger.info("Withdraw job so despoit data deleted!")

    
    # Get last signature to return 
    # activity = deposit_data['activity']
    #act_obj = ast.literal_eval(activity)
    print(deposit_data['activity'])
    activity = json.loads(deposit_data['activity'])
    
    
    last_activity = list(activity)[-1]
    print("Last Activity")
    print(json.dumps(last_activity))
    
    
    logger.info("This job is officially closed and deleted!") 
    
    return {
        'statusCode' : 200,
        'body': {
            'status':'done',
            'message':'Job closed and deleted',
            'signature': last_activity['signature']
        }
    }
    
    

def get_deposit(wallet):
        """
        Gets deposit data from table 

        :param wallet: Wallet string of user.
        :return: The deposit data row
        """
        try:
            response = depositsDB.get_item(Key={"wallet": wallet})
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
            
def delete_deposit(wallet):
        """
        Delete deposit info for wallet

        :param wallet: updating wallet deposit (Key)
        :return: Boolean of success or failure.
        """
        try:
            depositsDB.delete_item(Key={"wallet": wallet})
        except botocore.exceptions.ClientError as err:
            logger.error(
                "Couldn't delete deposit! Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            return False
        else:
            return True
