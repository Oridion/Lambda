import ast
import os
import json
import datetime
import asyncio
import logging
import boto3
import botocore
from anchorpy import Provider
from anchorpy import Wallet
from solana.transaction import Transaction
from solana.rpc.types import TxOpts
from solana.rpc.commitment import Confirmed
from solana.rpc.api import Client
from solana.rpc.async_api import AsyncClient
from solana.rpc.websocket_api import connect
from solders.pubkey import Pubkey
from solders.keypair import Keypair
from solders.compute_budget import set_compute_unit_limit
from solders.compute_budget import set_compute_unit_price
from anchor.instructions import withdraw

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#DynamoDB
dynamodb = boto3.resource('dynamodb')
depositsDB = dynamodb.Table("deposits")
jobsDB = dynamodb.Table("jobs")

#SNS
snsClient = boto3.client('sns')

#we should get this from environment variables.
oridion_program_id = Pubkey.from_string(os.environ['ORD_PROGRAM_ADDRESS'])

#wss url
wss_url = os.environ['WSS_URL']

#Set Client and async client (devnet_env or mainnet_env)
http_client = Client(os.environ['MAINNET_ENV'])
async_client = AsyncClient(os.environ['MAINNET_ENV'])

# Backup RPC 
backup_http_client = Client(os.environ['BACKUP_RPC'])
backup_async_client = AsyncClient(os.environ['BACKUP_RPC'])

# Pass wallet, hop transaction, to planet name 
def lambda_handler(event, context):
    # print(json.dumps(event, indent=2))
    for record in event['Records']:
        process_task(record)

def process_task(record):
        
    # print(f"DynamoDB Record: {json.dumps(record['dynamodb'])}")
    
    # log record event ID
    print(f"Record Event ID: {record['eventID']}")

    if record['eventName'] != "INSERT":
        print("Event was not an INSERT. Rejecting job")
        return
    
    if 'NewImage' not in record['dynamodb']:
        print("NewImage not in record. Rejecting job")
        return
    
    # Set dbImage
    dbImage = record['dynamodb']['NewImage']
    
    if 'wallet' not in dbImage:
        print("Wallet not in record. Rejecting job")
        return
    
    if 'type' not in dbImage:
        print("Job type not in record. Rejecting job")
        return
    
    if 'destination' not in dbImage:
        print("Destination not in record. Rejecting job")
        return
    
    # Confirmm job type
    job_type = dbImage['type']['S']
    if job_type != "withdraw":
        print("Not a withdraw job. Rejecting job")
        return;

    ###### Record Validation Completed ######
    print("Job type is witdraw and record values validated.")
    
    # Set up wallet and destination
    wallet = dbImage['wallet']['S']
    destination = dbImage['destination']['S']

    # Log variables here for troubleshooting
    logger.info("Wallet: " + wallet)
    logger.info("Destination: " + destination)

    # Validate user pubkey is real
    wallet_pk = Pubkey.from_string(wallet)
    if not wallet_pk.is_on_curve:
        print("Wallet key not valid. Rejecting job")
        return
    logger.info("Wallet key valid")  
    
    # Validate destination pubkey is real
    destination_wallet_pk = Pubkey.from_string(destination)
    if not destination_wallet_pk.is_on_curve:
        print("Destination wallet key not valid. Rejecting job")
        return 
    logger.info("Destination wallet key valid")  

    print("Both from and destination public keys valided")

    ############## START PROCESSING ##############
    # Setup loop
    loop = asyncio.get_event_loop()
    
    # Setup manager keypair
    manager_kp = Keypair.from_base58_string(os.environ['MANAGER_SECRET'])
    manager_anchor_wallet = Wallet(manager_kp)
    
    # setup provider
    provider = Provider(async_client,manager_anchor_wallet,TxOpts(skip_confirmation=False,skip_preflight=True,preflight_commitment=Confirmed,max_retries=0))
    
    # backup provider
    backup_provider = Provider(backup_async_client,manager_anchor_wallet,TxOpts(skip_confirmation=False,skip_preflight=True,preflight_commitment=Confirmed,max_retries=0))
    
    # Get deposit - (To get the from planet and deposit lamports)
    deposit_data = get_deposit(wallet)
    if not deposit_data:
        print("Deposit data for wallet address not found")
        return
    logger.info("Deposit data found for wallet")
    
    # Gather necessary data from deposit db
    from_planet_name = deposit_data['loc']
    deposit_lamports = deposit_data['deposit']
    activity = deposit_data['activity']
  
    logger.info("DB | Location: : " + from_planet_name)
    logger.info("DB | Deposit: " +  str(deposit_lamports))
    
    # Set PDAs
    # From Planet pubkey
    from_planet_pda_seed = [b"_PLA_", from_planet_name.encode(), b"_NET_"]
    from_planet_pda, nonce = Pubkey.find_program_address(from_planet_pda_seed, oridion_program_id)
    logger.info(f"From Planet PDA: {from_planet_pda}")

    # Deposit, Wallet, and both planet names verified.
    logger.info("### All validations complete ####") 
    
    # Start withdraw anchor instruction 
    logger.info("Starting withdraw anchor transaction") 
  
    ix = withdraw(
        {"withdraw_lamports": int(deposit_lamports)}, 
        {
            "destination": destination_wallet_pk, 
            "from_planet": from_planet_pda,
            "manager": manager_kp.pubkey(), # signer
        }
    )
    
    #latest blockhash
    # latest_blockhash = http_client.get_latest_blockhash(Confirmed).value
    latest_blockhash = get_latest_blockhash_rpc()
    if not latest_blockhash:
        print("Failed to get latest blockhash! Exiting!")
        return;


    hash = latest_blockhash.blockhash
    last_valid_height = latest_blockhash.last_valid_block_height
    logger.info(f"Last valid height: {last_valid_height}")
    
    # Tx
    tx = Transaction()
    tx.recent_blockhash = hash
    
    # set fee payer
    tx.fee_payer = manager_kp.pubkey()
    
    # Add CU limit
    cu_limit = set_compute_unit_limit(3400)
    tx.add(cu_limit)
    
    #add Priority fee
    priority_fee = set_compute_unit_price(20000)
    tx.add(priority_fee)
    
    # Add transaction
    tx.add(ix)

    logger.info("Built anchor instruction for transaction") 
        
    #manager signed transaction
    signed_tx = manager_anchor_wallet.sign_transaction(tx)
    logger.info("Manager signed transaction successfully") 

    # Serialize tx
    serialized_tx = signed_tx.serialize()
    
    # if not signed_tx.verify_signatures():
    #     print("Transaction signature verification failed! Ending processing")
    #     return 
    
    logger.info("Submitting transaction..") 
    # Submit transaction
    loop.run_until_complete(submit_withdraw(provider,backup_provider,serialized_tx,last_valid_height))
    logger.info("Transaction submitted") 
    # if signature and type(signature) is dict:
    #     logger.error('ERROR Processing solana transaction')
    #     logger.error(f"ERROR MSG: {signature['errorMessage']['message']}")
    #     print(signature['errorMessage']['message'] + " | Ending transaction!")
    #     return 
        
    signature = signed_tx.signature()
    logger.info(f"Signature: {signature}")
    loop.run_until_complete(listen_transaction(signature))
    logger.info("Listener returned with transaction confirmation! Finishing up...")
    
    # --------------------------------- #
    #Datetime Now
    current_datetime = datetime.datetime.now()
    now = int(current_datetime.timestamp())
    
    # Update activity
    act_obj = ast.literal_eval(activity)
    new_item = {
        "action": "W", # HP = Hop planet
        "to": destination,
        "time": now,
        "signature": str(signature)
    }
    act_obj.append(new_item)
    updated_activity = json.dumps(act_obj)
    
    # Update Deposit in DB
    update_result = update_deposit(wallet,destination,now,updated_activity)
    if not update_result:
        # response_body['message'].append("There was an error updating depositsDB")
        print("There was an error updating depositsDB")
        return
    logger.info("Updated deposit with withdraw activity")
    

    # Update job to completed
    job_updated = update_job_to_completed(wallet)
    if not job_updated:
        print("Job was not update to completed! Something seriously wrong here!")
        return
    logger.info("Job marked completed!")

    snsMessage = job_type +  " task has been completed for " + wallet
    send_sns(snsMessage)
    print("SNS message sent")
    logger.info("Withdraw process completed!")    
    logger.info("Done") 
    return
    
    

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
            
            
def update_deposit(wallet,to_planet_name,now,updated_activity):
        """
        Updates wallet deposit info

        :param wallet: updating wallet deposit (Key)
        :param to_planet_name: Destination planet name
        :param now: timestamp of now
        :param updated_activity: string of array
        :return: Boolean of success or failure.
        """
        try:
            depositsDB.update_item(
                Key={"wallet": wallet},
                UpdateExpression="SET loc=:loc,last_updated=:last_updated, hops=hops + :increment, activity=:activity",
                ExpressionAttributeValues={":loc": to_planet_name, ":last_updated": now, ":increment" : 1, ":activity" : updated_activity},
                ReturnValues="UPDATED_NEW",
            )
        except botocore.exceptions.ClientError as err:
            logger.error(
                "Couldn't update deposit! Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            return False
        else:
            return True

            
async def listen_transaction(signature):
    async with connect(wss_url) as websocket:
        await websocket.signature_subscribe(signature,"confirmed")
        logger.info("Subscribed to the signature")
        first_resp = await websocket.recv()
        subscription_id = first_resp[0].result
        next_resp = await websocket.recv()
        logger.info(next_resp)
        await websocket.signature_unsubscribe(subscription_id)
        return next_resp
    

def update_job_to_completed(wallet):
    """
    Updates job to completed
    :param wallet: updating wallet deposit (Key)
    :return: Boolean of success or failure.
    """
    try:
        jobsDB.update_item(
            Key={"wallet": wallet},
            UpdateExpression="SET completed=:completed",
            ExpressionAttributeValues={":completed":1},
            ReturnValues="UPDATED_NEW",
        )
    except botocore.exceptions.ClientError as err:
        logger.error(
            "Couldn't update job! Here's why: %s: %s",
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        return False
    else:
        return True
    
def send_sns(snsMessage):
    snsClient.publish(TopicArn='arn:aws:sns:us-west-1:058264465436:TaskComplete',Message=snsMessage)
    return


# Submit transaction (Payer is Manager)
async def submit_withdraw(provider,backup_provider,serialized_tx,last_valid_height):
    blockheight = await get_block_height(provider,backup_provider)
    sent = 1
    while sent < 6 and blockheight < last_valid_height:
        # await provider.send(tx)
        try:
            await provider.connection.send_raw_transaction(serialized_tx,TxOpts(skip_preflight=True))
            logger.info(f"Sending Tx | Helius API | BH: {blockheight}")
        except:
            await backup_provider.connection.send_raw_transaction(serialized_tx,TxOpts(skip_preflight=True))
            logger.info(f"Sending Tx | BACKUP API | BH: {blockheight}")
        sent+=1
        await asyncio.sleep(1)
        blockheight = await get_block_height(provider,backup_provider)

    logger.info("submitted serialized tx while loop completed")
    return True

def get_latest_blockhash_rpc():
    try:
        blockhash = http_client.get_latest_blockhash(Confirmed).value
        logger.info("Received Blockhash from Helius API.")
        return blockhash
    except:
        logger.info("Error getting Blockhash from Helius API. Trying backup rpc..")
        try:
            blockhash = backup_http_client.get_latest_blockhash(Confirmed).value
            logger.info("Blockhash received from backup rpc!")
            return blockhash
        except:
            logger.info("Failed to get Blockhash from both Helius and Backup API")
            return False
        
        
async def get_block_height(provider,backup_provider):
    try:
        blockheight = (await provider.connection.get_block_height()).value
    except:
        logger.info("Error getting block height from Helius API. Trying backup rpc..")
        try:
            blockheight = (await backup_provider.connection.get_block_height()).value
            logger.info("Blockheight received from backup rpc!")
        except:
            logger.info("Failed to get Blockheight from both Helius and Backup API")
            blockheight = False
    return blockheight