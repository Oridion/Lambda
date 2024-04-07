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
from solders.signature import Signature
from anchor.accounts import Universe
from anchor.instructions import star_hop_two_start
from anchor.instructions import star_hop_two_end

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
db = dynamodb.Table("deposits")

#we should get this from environment variables.
universe_pda = Pubkey.from_string(os.environ['UNIVERSE_ADDRESS'])
oridion_program_id = Pubkey.from_string(os.environ['ORD_PROGRAM_ADDRESS'])

#Set Client and async client (devnet_env or mainnet_env)
http_client = Client(os.environ['DEVNET_ENV'])
async_client = AsyncClient(os.environ['DEVNET_ENV'])


# Pass wallet, hop transaction, to planet name 
def lambda_handler(event, context):
    
    
    # Response body
    response_body = {
        'status': 'error',
        'message': []
    } 
    
    # Setup loop
    loop = asyncio.get_event_loop()
    
    # Setup manager keypair
    manager_kp = Keypair.from_base58_string(os.environ['MANAGER_SECRET'])
    manager_anchor_wallet = Wallet(manager_kp)
    
    # default error messages array and valid marker
    valid = True
    
    # Validate post variables
    if 'wallet' not in event:
        response_body['message'].append("Wallet public key not found")
        valid = False
        
    if 'to' not in event:
        response_body['message'].append("Planet name not found")
        valid = False
        
    if not valid:
        logger.error('Post data validation error')
        return {
            'statusCode': 200,
            'body': response_body
        }
        
    
    # POST Variables
    wallet = event['wallet'] # Ci1f6bfbWVfbmaVvfinz7rcmYwaXYgWAiRciZanknE6U'
    to_planet_name = event['to'] # ANDORA, etc..
    logger.info("POST (wallet): " + wallet)
    logger.info("POST (to planet): " + to_planet_name)
    
    
    # Get deposit - (To get the from planet and deposit lamports)
    deposit_data = get_deposit(wallet)
    
    # Validate deposit data found
    if not deposit_data:
        response_body['message'].append("Wallet deposit not found")
        return {
            'statusCode': 200,
            'body': response_body
        }
    logger.info("Deposit data found for wallet")
    
    from_planet_name = deposit_data['loc']
    deposit_lamports = deposit_data['deposit']
    activity = deposit_data['activity']
    logger.info("DB | Location: : " + from_planet_name)
    logger.info("DB | Deposit: " +  str(deposit_lamports))
    
 
    # Validate user pubkey
    wallet_pk = Pubkey.from_string(wallet)
    if not wallet_pk.is_on_curve:
        response_body['message'].append("Wallet key not valid")
        return {
            'statusCode': 200,
            'body': response_body
        }
    logger.info("Wallet key valid")  
    
    
    # --------------------------------- #
    # Get Universe data
    # --------------------------------- #
    # Use asyncio loop run until complete to synchronously "await" an async function
    universe = loop.run_until_complete(get_universe())
    if not universe:
        response_body['message'].append("Universe not found")
        logger.error('Universe not found')
        return {
            'statusCode': 200,
            'body': response_body
        }
        
        
    # validate to planet name is in universe planets list.
    # We don't need to do from planet name because it's already been validated 
    # before it was entered into the DB. 
    if(to_planet_name not in universe.p):
        logger.error("To planet not in universe!") 
        response_body['message'].append("Planet not in universe")
        return {
            'statusCode': 200,
            'body': response_body
        }
    # --------------------------------- #
    logger.info("New destination planet name found in universe")
    
    
    
    # Set PDAs
    # To Planet pubkey
    to_planet_pda_seed = [b"_PLA_", to_planet_name.encode(), b"_NET_"]
    to_planet_pda, nonce = Pubkey.find_program_address(to_planet_pda_seed, oridion_program_id)
    
    # From Planet pubkey
    from_planet_pda_seed = [b"_PLA_", from_planet_name.encode(), b"_NET_"]
    from_planet_pda, nonce = Pubkey.find_program_address(from_planet_pda_seed, oridion_program_id)
    
    
    # Generate two star IDs
    star_one_id = id_generator()
    star_two_id = id_generator()
    
    # Get PDAs for stars
    s1_pda_seed = [b"_ST_", star_one_id.encode(), b"_AR_"]
    s2_pda_seed = [b"_ST_", star_two_id.encode(), b"_AR_"]
    s1_planet_pda, nonce = Pubkey.find_program_address(s1_pda_seed, oridion_program_id)
    s2_planet_pda, nonce = Pubkey.find_program_address(s2_pda_seed, oridion_program_id)
    
    # Log all PDAs
    logger.info(f"from_planet_pda: {from_planet_pda}")
    logger.info(f"s1_pda: {s1_planet_pda}")
    logger.info(f"s2_pda: {s2_planet_pda}")
    logger.info(f"to_planet_name: {to_planet_pda}")
    
    # Deposit, Wallet, and both planet names verified.
    logger.info("All validations complete") 
    
    # Start hop anchor instruction 
    logger.info("Starting hop anchor transaction") 
  
    # Star hop start Instruction
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
    
    # Star hop end Instruction
    ix2 = star_hop_two_end(
        {
            "to_planet": to_planet_pda,
            "star_one": s1_planet_pda,
            "star_two": s2_planet_pda,
            "manager": manager_kp.pubkey(), # signer
        }
    )
    
    #########################################################################
    #latest blockhash
    latest_blockhash = http_client.get_latest_blockhash(Confirmed).value
    hash_one = latest_blockhash.blockhash
    
    # logger.info(str(hash_one))
    
    # TX One 
    tx1 = Transaction(hash_one)
    tx1.add(ix1)
    logger.info("Built step one anchor instruction for transaction") 
    logger.info("Submitting hop start transaction") 
    
    #manager signed transaction
    signed_tx1 = manager_anchor_wallet.sign_transaction(tx1)
    logger.info("Manager signed first transaction successfully") 
    
    # Submit transaction
    signature1 = loop.run_until_complete(submit_tx(async_client,manager_anchor_wallet,signed_tx1))
    logger.info("Transaction one completed") 
    if signature1 and type(signature1) is dict:
        logger.error('ERROR Processing solana transaction')
        logger.error(f"ERROR MSG: {signature1['errorMessage']['message']}")
        response_body['message'].append(signature1['errorMessage']['message'])
        return {
            'statusCode': 200,
            'body': response_body
        }
        
    logger.info(f"Signature 1: {signature1}")
    
    
    # Now verify first transaction completed before moving the next transaction
    loop.run_until_complete(listen_transaction(signature1))
    logger.info("Listen returned for signature 1!")
    #########################################################################
    #########################################################################
    # Continue to second transaction
    
    #latest blockhash
    latest_blockhash_two = http_client.get_latest_blockhash(Confirmed).value
    hash_two = latest_blockhash_two.blockhash
        
    # TX Two
    tx2 = Transaction(hash_two)
    tx2.add(ix2)
    logger.info("Built step two anchor instruction for transaction") 
    logger.info("Submitting hop end transaction") 
    
    #manager signed transaction
    signed_tx2 = manager_anchor_wallet.sign_transaction(tx2)
    logger.info("Manager signed second transaction successfully") 
    
    # Submit transaction
    signature2 = loop.run_until_complete(submit_tx(async_client,manager_anchor_wallet,signed_tx2))
    logger.info("Transaction two completed") 
    if signature2 and type(signature2) is dict:
        logger.error('ERROR Processing solana transaction')
        logger.error(f"ERROR MSG: {signature2['errorMessage']['message']}")
        response_body['message'].append(signature2['errorMessage']['message'])
        return {
            'statusCode': 200,
            'body': response_body
        }
        
    logger.info(f"Signature 2: {signature2}")
    
    
    # Now verify first transaction completed before moving the next transaction
    loop.run_until_complete(listen_transaction(signature2))
    logger.info("Listen returned for signature 2!")
    #########################################################################
    
    # Now both transactions completed
    
    #Datetime Now
    current_datetime = datetime.datetime.now()
    now = int(current_datetime.timestamp())
    
    # Update activity
    act_obj = ast.literal_eval(activity)
    new_item = {
        "action": "HS2", # HP = Hop planet
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
        response_body['message'].append("There was an error updating db")
        return {
            'statusCode': 200,
            'body': response_body
        }

    logger.info("Data row updated successfully")    
    logger.info("Done") 
    
    # RETURN SUCCESS! 
    return {
        'statusCode' : 200,
        'body': {'status': 'success', 'message' : 'Hop data updated successfully', 'signature': str(signature1) + ':' + str(signature2)  }
    }
    
    

def get_deposit(wallet):
        """
        Gets deposit data from table 

        :param wallet: Wallet string of user.
        :return: The deposit data row
        """
        try:
            response = db.get_item(Key={"wallet": wallet})
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
    
# fetch universe account
async def get_universe():
    acc = await Universe.fetch(async_client, universe_pda)
    if acc is None:
        # the fetch method returns null when the account is uninitialized
        raise ValueError("account not found")
    return acc  

# Submit transaction (Payer is Manager)
async def submit_tx(async_client,manager_anchor_wallet,tx):
    provider = Provider(async_client,manager_anchor_wallet)
    sig = await provider.send(tx,TxOpts(preflight_commitment=Confirmed))
    # logger.info(sig)
    return sig
    
    
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
            db.update_item(
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
        

def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


async def listen_transaction(signature):
    async with connect("wss://api.devnet.solana.com") as websocket:
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