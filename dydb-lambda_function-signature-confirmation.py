import asyncio
import logging
import os
from solana.rpc.async_api import AsyncClient
from solana.rpc.websocket_api import connect

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Set Client and async client (devnet_env or mainnet_env)
async_client = AsyncClient(os.environ['MAINNET_ENV'])

#WSS URL
wss_url = os.environ['WSS_URL']

# Pass wallet, hop transaction, to planet name 
def lambda_handler(event, context):
    
    # Response body
    response_body = {
        'status': 'error',
        'message': []
    } 
    
    # Setup loop
    loop = asyncio.get_event_loop()
    

    # default error messages array and valid marker
    valid = True
    
    # Validate post variables
    if 'signature' not in event:
        response_body['message'].append("Signature not found")
        valid = False
        
    if not valid:
        logger.error('Post data validation error')
        return {
            'statusCode': 200,
            'body': response_body
        }
        
    # POST Variables
    signature = event['signature']        
    logger.info(f"Signature: {signature}")
    
    #########################################################################
    # Confirm signature 
    loop.run_until_complete(listen_transaction(signature))
    logger.info("Signature confirmed successfully!")
    #########################################################################    
    
    # RETURN SUCCESS! 
    return {
        'statusCode' : 200,
        'body': {'status':'success', 'message':'Signature confirmed' , 'code':'9' }
    }
    

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
        logger.info(next_resp)
        await websocket.signature_unsubscribe(subscription_id)
        return next_resp