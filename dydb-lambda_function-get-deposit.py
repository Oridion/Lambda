import logging
import boto3
import botocore
from solders.pubkey import Pubkey


logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
db = dynamodb.Table("deposits")


# Post variable should be wallet
def lambda_handler(event, context):

    print(db.table_status)

    # default error messages array and valid marker
    valid = True

    # Response body
    response_body = {
        'status': 'error',
        'message': []
    }

    # validate post data
    if 'wallet' not in event:
        response_body['message'].append("Wallet key not found")
        valid = False

    if not valid:
        return {
            'statusCode': 200,
            'body': response_body
        }
    # end validate post variables


    # POST variables
    user_public_key = event['wallet']
    logger.info("Requested deposit data for wallet: " + user_public_key)

    # Set user pubkey
    user_pk = Pubkey.from_string(user_public_key)
    if not user_pk.is_on_curve:
        response_body['message'].append("User wallet address not valid")
        return {
            'statusCode': 200,
            'body': response_body
        }
    logger.info("User wallet address valid")
    #-------------------------------------------------------------------#
    # Validations complete
    #-------------------------------------------------------------------#

    # --------------------------------- #
    # DYNAMO DB Connection
    logger.info("Connecting to Dynamodb...")
    result = get_deposit(user_public_key)
    logger.info("Returned item from DB")
    # --------------------------------- #
    logger.info("Done")


    # RETURN SUCCESS!
    return {
        'statusCode' : 200,
        'body': {'status': 'success', 'deposit' : result }
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
            return response["Item"]
