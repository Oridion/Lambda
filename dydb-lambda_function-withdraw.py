import os
import json
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

dynamodb = boto3.resource('dynamodb')
db = dynamodb.Table("deposits")

#we should get this from environment variables.
oridion_program_id = Pubkey.from_string(os.environ['ORD_PROGRAM_ADDRESS'])

#wss url
wss_url = os.environ['WSS_URL']

#Set Client and async client (devnet_env or mainnet_env)
http_client = Client(os.environ['MAINNET_ENV'])
async_client = AsyncClient(os.environ['MAINNET_ENV'])


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
        
    if 'destination' not in event:
        response_body['message'].append("Destination wallet name not found")
        valid = False
        
    if not valid:
        logger.error('Post data validation error')
        return {
            'statusCode': 200,
            'body': response_body
        }
        
    
    # POST Variables
    wallet = event['wallet'] # Ci1f6bfbWVfbmaVvfinz7rcmYwaXYgWAiRciZanknE6U'
    destination = event['destination'] # Ci1f6bfbWVfbmaVvfinz7rcmYwaXYgWAiRciZanknE6U'
    logger.info("POST (wallet): " + wallet)
    
    # Get deposit - (To get the from planet and deposit lamports)
    deposit_data = get_deposit(wallet)
    
    # Validate deposit data found
    if not deposit_data:
        response_body['message'].append("Deposit data for wallet address not found")
        return {
            'statusCode': 200,
            'body': response_body
        }
    logger.info("Deposit data found for wallet")
    
    from_planet_name = deposit_data['loc']
    deposit_lamports = deposit_data['deposit']
  
    logger.info("DB | Location: : " + from_planet_name)
    logger.info("DB | Deposit: " +  str(deposit_lamports))
    
    # Validate user pubkey is real
    wallet_pk = Pubkey.from_string(wallet)
    if not wallet_pk.is_on_curve:
        response_body['message'].append("Wallet key not valid")
        return {
            'statusCode': 200,
            'body': response_body
        }
    logger.info("Wallet key valid")  
    
    # Validate destination pubkey is real
    destination_wallet_pk = Pubkey.from_string(destination)
    if not destination_wallet_pk.is_on_curve:
        response_body['message'].append("Destination wallet key not valid")
        return {
            'statusCode': 200,
            'body': response_body
        }
    logger.info("Destination wallet key valid")  
    
    
    # Set PDAs
    # From Planet pubkey
    from_planet_pda_seed = [b"_PLA_", from_planet_name.encode(), b"_NET_"]
    from_planet_pda, nonce = Pubkey.find_program_address(from_planet_pda_seed, oridion_program_id)
    logger.info(f"From Planet PDA: {from_planet_pda}")

    # Deposit, Wallet, and both planet names verified.
    logger.info("All validations complete") 
    
    # Start withdraw anchor instruction 
    logger.info("Starting withdraw anchor transaction") 
  
    acc = [manager_kp]
    ix = withdraw(
        {"withdraw_lamports": int(deposit_lamports)}, 
        {
            "destination": destination_wallet_pk, 
            "from_planet": from_planet_pda,
            "manager": manager_kp.pubkey(), # signer
        }
    )
    
    #latest blockhash
    latest_blockhash = http_client.get_latest_blockhash(Confirmed).value
    hash = latest_blockhash.blockhash
    # logger.info(str(hash))
    
    # Tx
    tx = Transaction(hash)
    
    # set fee payer
    tx.fee_payer = manager_kp.pubkey()
    
    
    # Add CU limit
    cu_limit = set_compute_unit_limit(3400)
    tx.add(cu_limit)
    
    
    #add Priority fee
    priority_fee = set_compute_unit_price(9000)
    tx.add(priority_fee)
    
    
    # Add transaction
    tx.add(ix)

    
    logger.info("Built anchor instruction for transaction") 
    logger.info("Submitting transaction") 
    
    
    #manager signed transaction
    signed_tx = manager_anchor_wallet.sign_transaction(tx)
    logger.info("Manager signed transaction successfully") 
    
    if not tx.verify_signatures():
        response_body['message'].append("Transaction signature verification failed!")
        return {
            'statusCode': 200,
            'body': response_body
        }
    
    
    # Submit transaction
    signature = loop.run_until_complete(submit_withdraw(async_client,manager_anchor_wallet,tx))
    logger.info("Transaction completed") 
    if signature and type(signature) is dict:
        logger.error('ERROR Processing solana transaction')
        logger.error(f"ERROR MSG: {signature['errorMessage']['message']}")
        response_body['message'].append(signature['errorMessage']['message'])
        return {
            'statusCode': 200,
            'body': response_body
        }
        
        
    ### This is the end of first half
        
    logger.info(f"Signature: {signature}")
    
    return_from_listen = loop.run_until_complete(listen_transaction(signature))
    logger.info("Listen returned!")

    # --------------------------------- #
    # Delete Deposit
    delete_result = delete_deposit(wallet)
    if not delete_result:
        response_body['message'].append("There was an error deleting deposit from db")
        return {
            'statusCode': 200,
            'body': response_body
        }

    logger.info("Data row deleted successfully")    
    logger.info("Done") 
    
    # RETURN SUCCESS! 
    return {
        'statusCode' : 200,
        'body': {'status': 'success', 'message' : 'All deposit data deleted successfully', 'signature': str(signature)  }
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
    

# Submit withdraw transaction (Payer is Manager)
async def submit_withdraw(async_client,manager_anchor_wallet,tx):
    provider = Provider(async_client,manager_anchor_wallet)
    sig = await provider.send(tx,TxOpts(skip_confirmation=True,preflight_commitment=Confirmed))
    return sig
    
    
def delete_deposit(wallet):
        """
        Delete deposit info for wallet

        :param wallet: updating wallet deposit (Key)
        :return: Boolean of success or failure.
        """
        
        try:
            db.delete_item(Key={"wallet": wallet})
        except botocore.exceptions.ClientError as err:
            logger.error(
                "Couldn't delete deposit! Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            return False
        else:
            return True
            
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