import ast
import json
import datetime
import string
import random
import asyncio
import logging
import boto3
import botocore
import os
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
from anchor.accounts import Universe
from anchor.instructions import star_hop_three_start
from anchor.instructions import star_hop_two_start
from anchor.instructions import star_hop_three_end
from anchor.instructions import star_hop_two_end

#Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#DynamoDB
dynamodb = boto3.resource('dynamodb')
depositsDB = dynamodb.Table("deposits")
jobsDB = dynamodb.Table("jobs")

#SNS
snsClient = boto3.client('sns')

#we should get this from environment variables.
universe_pda = Pubkey.from_string(os.environ['UNIVERSE_ADDRESS'])
oridion_program_id = Pubkey.from_string(os.environ['ORD_PROGRAM_ADDRESS'])

#Set Client and async client (devnet_env or mainnet_env)
http_client = Client(os.environ['MAINNET_ENV'])
async_client = AsyncClient(os.environ['MAINNET_ENV'])

# Backup RPC 
backup_http_client = Client(os.environ['BACKUP_RPC'])
backup_async_client = AsyncClient(os.environ['BACKUP_RPC'])

#WSS URL
wss_url = os.environ['WSS_URL']

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
    
    ###### Record Validation Completed ######
    print("Record values validation completed. Continuing processing..")

    
    # Confirmm job type
    job_type = dbImage['type']['S']
    if job_type == "withdraw":
        print("Withdraw job found. No longer processing")
        return;
    
    # Set up wallet and destination
    wallet = dbImage['wallet']['S']
    to_planet_name = dbImage['destination']['S']
    
    # Setup loop
    loop = asyncio.get_event_loop()
    
    # Setup manager keypair
    manager_kp = Keypair.from_base58_string(os.environ['MANAGER_SECRET'])
    manager_anchor_wallet = Wallet(manager_kp)
    
    # setup provider
    provider = Provider(async_client,manager_anchor_wallet,TxOpts(skip_confirmation=False,skip_preflight=True,preflight_commitment=Confirmed,max_retries=0))
    
    # backup provider
    backup_provider = Provider(backup_async_client,manager_anchor_wallet,TxOpts(skip_confirmation=False,skip_preflight=True,preflight_commitment=Confirmed,max_retries=0))


    logger.info("POST (wallet): " + wallet)
    logger.info("POST (to planet): " + to_planet_name)
    
    # Get deposit - (To get the from planet and deposit lamports)
    deposit_data = get_deposit(wallet)
    
    # Validate deposit data found
    if not deposit_data:
        # response_body['message'].append("Wallet deposit not found")
        print("ERROR: Wallet deposit not found")
        return
    
    logger.info("Deposit data found for wallet")
    
    from_planet_name = deposit_data['loc']
    deposit_lamports = deposit_data['deposit']
    activity = deposit_data['activity']
    logger.info("DB | Location: : " + from_planet_name)
    logger.info("DB | Deposit: " +  str(deposit_lamports))
    
    
    # Set PDAs
    # To Planet pubkey
    to_planet_pda_seed = [b"_PLA_", to_planet_name.encode(), b"_NET_"]
    to_planet_pda, nonce = Pubkey.find_program_address(to_planet_pda_seed, oridion_program_id)
    
    # From Planet pubkey
    from_planet_pda_seed = [b"_PLA_", from_planet_name.encode(), b"_NET_"]
    from_planet_pda, nonce = Pubkey.find_program_address(from_planet_pda_seed, oridion_program_id)
    
    # Get instructions
    ix_array = get_hop_instruction(job_type,from_planet_pda,to_planet_pda,manager_kp,deposit_lamports)
    
    #########################################################################
    #TX 1: latest blockhash
    latest_blockhash = get_latest_blockhash_rpc()
    if not latest_blockhash:
        logger.info("Error getting latest blockhash")
        return
    
    hash_one = latest_blockhash.blockhash
    last_valid_height = latest_blockhash.last_valid_block_height
    
    #set up compute unit price 
    cu_limit_one = get_compute_unit("start",job_type)
    cu_limit_two = get_compute_unit("end",job_type)

    #set up Priority fee
    priority_fee = set_compute_unit_price(25000)
    
    # TX One 
    tx1 = Transaction()
    tx1.recent_blockhash = hash_one
    
    # set fee payer
    tx1.fee_payer = manager_kp.pubkey()
    
    # Set compute unit price 
    tx1.add(cu_limit_one)
    
    # Add Priority fee
    tx1.add(priority_fee)
    
    # Add instruction
    tx1.add(ix_array[0])

    # Manager signed transaction
    signed_tx1 = manager_anchor_wallet.sign_transaction(tx1)
    logger.info("Manager signed first transaction successfully") 
    
    # Serialize tx
    serialized_tx1 = signed_tx1.serialize()

    logger.info("Built step 1 instruction. Submitting transaction..") 

    # Submit transaction
    tx1_status = loop.run_until_complete(submit_tx(provider,backup_provider,serialized_tx1,last_valid_height))
    if not tx1_status:
        print("Failed submitting transaction 1! Ending!")
        return;

    
    logger.info("Submitting Transaction 1 completed") 
        
    signature1 = signed_tx1.signature()
    logger.info(f"Signature 1: {signature1}")

    # Now verify first transaction completed before moving the next transaction
    loop.run_until_complete(listen_transaction(signature1))
    logger.info("Listen returned for signature 1!")


    # sig1_json = json.loads(sig1_data)
    # logger.info("JSON 1 LOADED")

    #########################################################################
    #########################################################################
    # Continue to second transaction
    
    #latest blockhash
    latest_blockhash_two = get_latest_blockhash_rpc()
    if not latest_blockhash_two:
        logger.info("Error getting latest blockhash")
        return

    hash_two = latest_blockhash_two.blockhash
    last_valid_height = latest_blockhash.last_valid_block_height
    logger.info(f"Last valid height: {last_valid_height}")
        
    # TX Two
    tx2 = Transaction()
    tx2.recent_blockhash = hash_two
    
    # Set fee payer
    tx2.fee_payer = manager_kp.pubkey()
    
    # Add cu limit 
    tx2.add(cu_limit_two)
    
    # Add Priority fee
    tx2.add(priority_fee)
    
    # Add instruction
    tx2.add(ix_array[1])
    
    #manager signed transaction
    signed_tx2 = manager_anchor_wallet.sign_transaction(tx2)
    logger.info("Manager signed second transaction successfully") 
    
    # Serialize tx
    serialized_tx2 = signed_tx2.serialize()
    
    logger.info("Built step 2 transaction. Submitting transaction..") 

    # Submit transaction
    tx2_status = loop.run_until_complete(submit_tx(provider,backup_provider,serialized_tx2,last_valid_height))
    if not tx2_status:
        print("Failed submitting transaction 2! Ending!")
        return;
    
    logger.info("Submitting Transaction 2 completed") 

    # Set signature two
    signature2 = signed_tx2.signature()    
    logger.info(f"Signature 2: {signature2}")
    
    # Now verify first transaction completed before moving the next transaction
    loop.run_until_complete(listen_transaction(signature2))
    logger.info("Listen returned for signature 2!")

    # sig2_json = json.loads(sig2_data)
    # logger.info("JSON 2 LOADED")

    #########################################################################
    
    # Now both transactions completed
    
    #Datetime Now
    current_datetime = datetime.datetime.now()
    now = int(current_datetime.timestamp())
    
    # Update activity
    act_obj = ast.literal_eval(activity)
    new_item = {
        "action": "HS3", # HP = Hop planet
        "to": to_planet_name,
        "time": now,
        "signature": str(signature1) + ':' + str(signature2)
    }
    act_obj.append(new_item)
    updated_activity = json.dumps(act_obj)
    
 
    # --------------------------------- #
    # Update Deposit in DB
    update_result = update_deposit(wallet,to_planet_name,now,updated_activity)
    if not update_result:
        # response_body['message'].append("There was an error updating depositsDB")
        print("There was an error updating depositsDB")
        return
        

    # Update job to completed
    job_updated = update_job_to_completed(wallet)
    if not job_updated:
        return
    
    logger.info("Job set to completed!")

    snsMessage = job_type +  " task has been completed for " + wallet
    send_sns(snsMessage)

    logger.info("Hop successfully completed!")    
    logger.info("Done!") 
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
    
# # fetch universe account
# async def get_universe_old():
#     acc = await Universe.fetch(async_client, universe_pda)
#     if acc is None:
#         # the fetch method returns null when the account is uninitialized
#         raise ValueError("account not found")
#     return acc  

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


def update_deposit(wallet, to_planet_name,now,updated_activity):
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
                UpdateExpression="SET loc=:loc, last_updated=:last_updated, hops=hops + :increment, activity=:activity",
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
        

def update_job_to_completed(wallet):
        """
        Updates wallet job to completed
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


def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

async def listen_transaction(signature):
    async with connect(wss_url) as websocket:
        #finalized is about 16 seconds
        #confirmed is about 6 seconds
        await websocket.signature_subscribe(signature,"confirmed")
        logger.info("Subscribed to the signature")

        first_resp = await websocket.recv()
        subscription_id = first_resp[0].result
        next_resp = await websocket.recv()
        logger.info("Received data")
        # logger.info(subscription_id) //Receiving correctly
        logger.info(next_resp)
        await websocket.signature_unsubscribe(subscription_id)
        return next_resp
    

def send_sns(snsMessage):
    snsClient.publish(TopicArn='arn:aws:sns:us-west-1:058264465436:TaskComplete',Message=snsMessage)
    print("Message published")
    return


def get_compute_unit(instruction_type,job_type):
        if job_type == "star_two":
            if instruction_type == "start":
                return set_compute_unit_limit(33000)
            else:
                return set_compute_unit_limit(6300)
            
        if job_type == "star_three":
            if instruction_type == "start":
                return set_compute_unit_limit(60000)
            else:
                return set_compute_unit_limit(9000)


# Get instruction depending on job type
# It can star_two or star_three
def get_hop_instruction(job_type,from_planet_pda,to_planet_pda,manager_kp,deposit_lamports):

    instructions = []

    # Generate two star IDs
    star_one_id = id_generator()
    star_two_id = id_generator()

    # Get PDAs for stars
    s1_pda_seed = [b"_ST_", star_one_id.encode(), b"_AR_"]
    s2_pda_seed = [b"_ST_", star_two_id.encode(), b"_AR_"]
    s1_planet_pda, nonce = Pubkey.find_program_address(s1_pda_seed, oridion_program_id)
    s2_planet_pda, nonce = Pubkey.find_program_address(s2_pda_seed, oridion_program_id)

    if job_type == "star_two":
        # Log all PDAs
        logger.info(f"from_planet_pda: {from_planet_pda}")
        logger.info(f"s1_pda: {s1_planet_pda}")
        logger.info(f"s2_pda: {s2_planet_pda}")
        logger.info(f"to_planet_pda: {to_planet_pda}")
        ix1 = star_hop_two_start(
            {
                "star_one": star_one_id,
                "star_two": star_two_id,
                "deposit": int(deposit_lamports)
            }, 
            {
                "from_planet": from_planet_pda,
                "star_one": s1_planet_pda,
                "star_two": s2_planet_pda,
                "manager": manager_kp.pubkey(), # signer
            }
        )
        
        ix2 = star_hop_two_end(
            {
                "deposit": int(deposit_lamports)
            },
            {
                "to_planet": to_planet_pda,
                "star_one": s1_planet_pda,
                "star_two": s2_planet_pda,
                "manager": manager_kp.pubkey(), # signer
            }
        )


    # If job type is star_three we generate one more. 
    if job_type == "star_three":
        star_three_id = id_generator()
        s3_pda_seed = [b"_ST_", star_three_id.encode(), b"_AR_"]
        s3_planet_pda, nonce = Pubkey.find_program_address(s3_pda_seed, oridion_program_id)

        # Log all PDAs
        logger.info(f"from_planet_pda: {from_planet_pda}")
        logger.info(f"s1_pda: {s1_planet_pda}")
        logger.info(f"s2_pda: {s2_planet_pda}")
        logger.info(f"s3_pda: {s3_planet_pda}")
        logger.info(f"to_planet_pda: {to_planet_pda}")

        ix1 = star_hop_three_start(
            {
                "star_one": star_one_id,
                "star_two": star_two_id,
                "star_three": star_three_id,
                "deposit": int(deposit_lamports)
            }, 
            {
                "from_planet": from_planet_pda,
                "star_one": s1_planet_pda,
                "star_two": s2_planet_pda,
                "star_three": s3_planet_pda,
                "manager": manager_kp.pubkey(), # signer
            }
        )

        ix2 = star_hop_three_end(
            {
                "deposit": int(deposit_lamports)
            },
            {
                "to_planet": to_planet_pda,
                "star_one": s1_planet_pda,
                "star_two": s2_planet_pda,
                "star_three": s3_planet_pda,
                "manager": manager_kp.pubkey(), # signer
            }
        )

    instructions.append(ix1)
    instructions.append(ix2)
    return instructions
    

# Submit transaction (Payer is Manager)
async def submit_tx(provider,backup_provider,serialized_tx,last_valid_height):
    
    blockheight = await get_block_height(provider,backup_provider)
    if not blockheight:
        return False
    
    #default sent: 1
    sent = 1
    
    while sent < 6 and blockheight < last_valid_height:
        logger.info(f"Sending Tx | BH: {blockheight}")
        # await provider.send(tx)
        try:
            await provider.connection.send_raw_transaction(serialized_tx,TxOpts(skip_preflight=True))
            logger.info(f"Sending Tx | Helius | BH: {blockheight}")
        except:
            await backup_provider.connection.send_raw_transaction(serialized_tx,TxOpts(skip_preflight=True))
            logger.info(f"Sending Tx | BACKUP | BH: {blockheight}")
        sent+=1
        await asyncio.sleep(1.5)
        blockheight = await get_block_height(provider,backup_provider)

    logger.info("submitted serialized tx while loop completed")
    return True


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