import os
import json
import datetime
import asyncio
import logging
import boto3
import botocore
from solana.rpc.api import Client
from solana.rpc.async_api import AsyncClient
from solana.rpc.websocket_api import connect
from solders.pubkey import Pubkey
from solders.signature import Signature
from anchor.accounts import Universe

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#we should get this from environment variables.
universe_pda = Pubkey.from_string(os.environ['UNIVERSE_ADDRESS'])
treasury_public_key = Pubkey.from_string(os.environ['TREASURY_ADDRESS'])
oridion_program_id = Pubkey.from_string(os.environ['ORD_PROGRAM_ADDRESS'])

#Set Client and async client (devnet_env or mainnet_env)
http_client = Client(os.environ['DEVNET_ENV'])
async_client = AsyncClient(os.environ['DEVNET_ENV'])

def lambda_handler(event, context):
    
    #Datetime Now
    current_datetime = datetime.datetime.now()
    now = int(current_datetime.timestamp())
    
    dynamodb = boto3.resource('dynamodb')
    db = dynamodb.Table("deposits")
    # logger.info(db.table_status)
    
    
    # default error messages array and valid marker
    valid = True
    
    #setup loop
    loop = asyncio.get_event_loop()
    
    # Error messages
    error_messages = []
    
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
            'body': json.dumps(response_body)
        }
        

    
    # validate post data
    if 'user' not in event:
        response_body['message'].append("User key not found")
        valid = False

    if 'planet' not in event:
        response_body['message'].append("Planet name not found")
        valid = False

    if 'signature' not in event:
        response_body['message'].append("Signature not found")
        valid = False

    if not valid:
        # print(json.dumps(response_body))
        return {
            'statusCode': 200,
            'body': json.dumps(response_body)
        }
    # end validate post variables

    # POST variables
    user_public_key = event['user']
    planet_name = event['planet']
    signature = event['signature']
    logger.info("POST (user_public_key): " + user_public_key)
    logger.info("POST (planet_name): " + planet_name)
    logger.info("POST (signature): " + signature)
    logger.info("Connecting to http client.")
    
    sig = Signature.from_string(signature)
    loop.run_until_complete(listen_transaction(sig))
    logger.info("Listen returned!")
    
    # Validate that we can connect to solana
    
    response = http_client.get_transaction(sig,"json","confirmed",0).value
    logger.info("Connection responded.")
    #If no response then not able to connect
    if(response == None):
        response_body['message'].append("Signature not found in Solana")
        # print(json.dumps(response_body))
        return {
            'statusCode': 200,
            'body': json.dumps(response_body)
        }
        
    logger.info("Connection to Solana completed!")   
    # END signature validation
    # We are now connected and transaction went through

    # Setup accounts array 
    t = response.transaction
    m = response.transaction.meta
    h = t.transaction.message.header
    account_keys = t.transaction.message.account_keys
    accounts_count = len(account_keys)
    pre_balances = m.pre_balances
    post_balances = m.post_balances
    
    # Set user pubkey
    user_pk = Pubkey.from_string(user_public_key)
      
    # Planet pubkey
    planet_pda_seed = [b"_PLA_", planet_name.encode(), b"_NET_"]
    planet_pda, nonce = Pubkey.find_program_address(planet_pda_seed, oridion_program_id)
    # print(planet_pda)
    
    
    ### OTHER VALIDATION TESTS ### 
    if (accounts_count != 7):
        response_body['message'].append("Accounts count is not correct")
        valid = False
    
    if h.num_required_signatures != 1 or h.num_readonly_signed_accounts != 0 or h.num_readonly_unsigned_accounts != 3:
        print(h.num_readonly_signed_accounts)
        response_body['message'].append("Number of required accounts is not correct")
        valid = False
        
    # Check universe, treasury and user keys are correct and set in the transaction. 
    if oridion_program_id not in account_keys:
        response_body['message'].append("Oridion program key not found")
        valid = False
        
    if user_pk not in account_keys:
        response_body['message'].append("User public key not found")
        valid = False
    
    if universe_pda not in account_keys:
        response_body['message'].append("Universe public key not found")
        valid = False
        
    if treasury_public_key not in account_keys:
        response_body['message'].append("Treasury public key not found")
        valid = False
        
    if planet_pda not in account_keys:
        response_body['message'].append("Planet PDA key not found")
        valid = False

    # IF VALIDATION FAILS
    if not valid:
        response_body = {
            'status': 'error',
            'message': error_messages
        }
        # print(json.dumps(response_body))
        return {
            'statusCode': 200,
            'body': json.dumps(response_body)
        }
        
    logger.info("All required accounts found in anchor account list")  

    # Validating deposit amount 
    # Get indexes for account
    user_pk_idx = account_keys.index(user_pk);
    planet_idx = account_keys.index(planet_pda)

    #Get user deposit from balance change (minus the fee)  
    user_balance_difference = pre_balances[user_pk_idx] - post_balances[user_pk_idx] - m.fee
    # print(user_balance_difference)

    # Get the planet balance difference
    planet_balance_difference = post_balances[planet_idx] - pre_balances[planet_idx]
    
    #Make sure the balance difference on both the deposit and planet difference is the same
    if(planet_balance_difference != user_balance_difference):
        response_body = {
            'status': 'error',
            'message': ['Planet balance change and deposit not the same']
        }
        # print(json.dumps(response_body))
        return {
            'statusCode': 200,
            'body': json.dumps(response_body)
        }
        
    logger.info("All basic validations passed!")  
    #-------------------------------------------------------------------#
    # Validations are all now complete
    # From here down:
    # Deposit has been validated and is the same as the balance change in planet
    # From here down, user_balance_difference == deposit lamports
    #-------------------------------------------------------------------#
    
    
    # --------------------------------- #
    # Get Universe data
    # --------------------------------- #
    # Use asyncio loop run until complete to synchronously "await" an async function
    universe = loop.run_until_complete(get_universe())
    if not universe:
        response_body['message'].append("Universe not found")
        # print(json.dumps(error_status))
        return {
            'statusCode': 200,
            'body': json.dumps(response_body)
        }
        
    # --------------------------------- #
    logger.info("Obtained universe account") 
    
    # --------------------------------- #
    # Final check is to make sure the planet is in fact in the universe planets list
    if(planet_name not in universe.p):
        logger.info("Planet not in universe!") 
        response_body['message'].append("Planet not in universe")
        return {
            'statusCode': 200,
            'body': json.dumps(response_body)
        }
    # --------------------------------- #
    logger.info("planet name found in universe") 
    
    
    activity = [
        {
            'action': 'D',
            'to': planet_name,
            'time': str(now),
            'signature' : signature
        }
    ]
    
    
    # --------------------------------- #
    # Dynamo DB Connection
    logger.info("Submitting to DB...")
    try:
        db.put_item(
            Item={
                "wallet": user_public_key,
                "deposit": user_balance_difference,
                "hpfe": universe.hpfe,
                "hsfe2": universe.hsfe2,
                "hsfe3": universe.hsfe3,
                "wfe": universe.wfe,
                "loc": planet_name,
                "hops" : 2,
                "created": now,
                "last_updated": now,
                "activity" : json.dumps(activity)
            },
            ConditionExpression='attribute_not_exists(wallet)',
        )
    except botocore.exceptions.ClientError as err:
        logger.error(
            "Couldn't add deposit. Error: %s: %s",
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        response_body['message'].append("Record error")
        return {
            'statusCode': 200,
            'body': json.dumps(response_body)
        }
        
    logger.info("Deposit created successfully")    
    # --------------------------------- #
    logger.info("Done") 
    
    # RETURN SUCCESS! 
    return {
        'statusCode' : 200,
        'body': {'status': 'success', 'message' : 'Deposit was created successfully' }
    }
    
    
# fetch universe account
async def get_universe():
    acc = await Universe.fetch(async_client, universe_pda)
    if acc is None:
        # the fetch method returns null when the account is uninitialized
        raise ValueError("account not found")
    return acc  

async def listen_transaction(signature):
    async with connect("wss://api.devnet.solana.com") as websocket:
        #finalized is about 16 seconds
        #confirmed is about 6 seconds
        await websocket.signature_subscribe(signature,"finalized")
        logger.info("Subscribed to the signature")
        first_resp = await websocket.recv()
        subscription_id = first_resp[0].result
        next_resp = await websocket.recv()
        logger.info("Received data")
        # logger.info(subscription_id) //Receiving correctly
        logger.info(next_resp)
        await websocket.signature_unsubscribe(subscription_id)
        return next_resp