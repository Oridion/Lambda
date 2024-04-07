import ast
import json
import datetime
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
from solders.pubkey import Pubkey
from solders.keypair import Keypair
from solders.signature import Signature
from anchor.accounts import Universe
from anchor.instructions import planet_hop

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
    # print(manager_kp.pubkey())
    
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
    
    logger.info(from_planet_pda)
    logger.info(to_planet_name)
    
    # Deposit, Wallet, and both planet names verified.
    logger.info("All validations complete") 
    
    # Start hop anchor instruction 
    logger.info("Starting hop anchor transaction") 
  
    acc = [manager_kp]
    ix = planet_hop(
        {"lamports": int(deposit_lamports)}, 
        {
            "to_planet": to_planet_pda, 
            "from_planet": from_planet_pda,
            "manager": manager_kp.pubkey(), # signer
        }
    )
    
    #latest blockhash
    latest_blockhash = http_client.get_latest_blockhash(Confirmed).value
    hash = latest_blockhash.blockhash
    
    logger.info(str(hash))
    
    # Tx - TODO: SET THE block HASH!!!!
    tx = Transaction(hash)
    tx.add(ix)
    logger.info("Built anchor instruction for transaction") 
    logger.info("Submitting transaction") 
    
    #manager signed transaction
    signed_tx = manager_anchor_wallet.sign_transaction(tx)
    logger.info("Manager signed transaction successfully") 
    
    # Submit transaction
    signature = loop.run_until_complete(submit_hop_planet(async_client,manager_anchor_wallet,signed_tx,acc))
    logger.info("Transaction completed") 
    if signature and type(signature) is dict:
        logger.error('ERROR Processing solana transaction')
        logger.error(f"ERROR MSG: {signature['errorMessage']['message']}")
        response_body['message'].append(signature['errorMessage']['message'])
        return {
            'statusCode': 200,
            'body': response_body
        }
        
    logger.info(f"Signature: {signature}")
    
    
    #Datetime Now
    current_datetime = datetime.datetime.now()
    now = int(current_datetime.timestamp())
    
    # Update activity
    act_obj = ast.literal_eval(activity)
    new_item = {
        "action": "HP", # HP = Hop planet
        "to": to_planet_name,
        "time": now,
        "signature": str(signature)
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
        'body': {'status': 'success', 'message' : 'Deposit was updated successfully', 'signature': str(signature)}
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

# Submit hop planet transaction (Payer is Manager)
async def submit_hop_planet(async_client,manager_anchor_wallet,tx,acc):
    # provider = Provider(async_client,manager_anchor_wallet,opts=TxOpts(skip_confirmation=False, preflight_commitment=Confirmed))
    provider = Provider(async_client,manager_anchor_wallet)
    # logger.info(json.dumps(provider))
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