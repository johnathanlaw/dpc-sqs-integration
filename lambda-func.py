import requests
import json
import time
import boto3
from botocore.exceptions import ClientError

# Things to change!
MATILLION_REGION = 'eu1'

MATILLION_API_SECRET = {
    'aws-secret-name': 'greenwave-api',
    'aws-region': 'eu-west-1',
    'client_id_key': 'MATILLION_CLIENT_ID',
    'client_id_secret': 'MATILLION_CLIENT_SECRET'
}

DPC_PROJECT_NAME_TO_ID_MAP = {
    'My first project': 'c28506c2-66eb-4acb-9427-9665a461da71'
}

# Changes should be done above, no more required!
# Define the cache to store the access token and OAuth credentials
cache = {
    'auth':{
        'access_token': None,
        'expires_at': None
    },
    'oauth': {
        'client_id': None,
        'client_secret': None
    }
}

def get_matillion_oauth():
    # Check the cache to see if the token is available already, to avoid hitting AWS Secrets Manager unnecessarily
    # If not available:
        # Get the given secret (the aws-secret-name secret in the aws-region) from AWS Secret Manager
        # Save the values in the secret that are in the keys under client_id_key and client_id_secret in the cache

    if cache['oauth']['client_id'] and cache['oauth']['client_secret']:
        return cache['oauth']['client_id'], cache['oauth']['client_secret']
    else:
        # Create a Secrets Manager client
        print('Getting Boto3 session')
        session = boto3.session.Session()

        print('Setting up client for secrets manager')
        client = session.client(
            service_name='secretsmanager',
            region_name=MATILLION_API_SECRET['aws-region']
        )

        try:
            print(f'Attempting to get secret {MATILLION_API_SECRET['aws-secret-name']}')
            get_secret_value_response = client.get_secret_value(
                SecretId=MATILLION_API_SECRET['aws-secret-name']
            )
        except Exception as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print('The requested secret ' + MATILLION_API_SECRET['name'] + ' was not found')
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                print('The request was invalid due to:', e)
            else :
                print('The request failed because:', e)

        secret = json.loads(get_secret_value_response['SecretString'])
        cache['oauth']['client_id'] = secret[MATILLION_API_SECRET['client_id_key']]
        cache['oauth']['client_secret'] = secret[MATILLION_API_SECRET['client_id_secret']]

        return cache['oauth']['client_id'], cache['oauth']['client_secret']


def get_matillion_access_token():
    MATILLION_TOKEN_URL = 'https://id.core.matillion.com/oauth/dpc/token'

    try:
        currect_time = int(time.time())
        print('Current time: ', int(time.time()))
        print('Expires at: ', cache['auth']['expires_at'])
        if not cache['auth']['access_token'] or currect_time > cache['auth']['expires_at']:
            MATILLION_CLIENT_ID, MATILLION_CLIENT_SECRET = get_matillion_oauth()

            # Prepare the payload to request an access token
            token_payload = {
                'grant_type': 'client_credentials',
                'client_id': MATILLION_CLIENT_ID,
                'client_secret': MATILLION_CLIENT_SECRET
            }

            # Make the request to get the access token
            print('Requesting access token')
            response = requests.post(
                MATILLION_TOKEN_URL,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                data=token_payload
            )

            # Check if the token request was successful
            if response.status_code == 200:
                print('Save access token')
                token_data = response.json()
                cache['auth']['access_token'] = token_data['access_token']

                # Calculate the time when the token will expire
                # Save the access token and the time when it expires in the cache
                expires_at = int(time.time()) + (token_data['expires_in'] - 60)
                cache['auth']['expires_at'] = expires_at

                return cache['auth']['access_token']
            else:
                raise Exception(f'Failed to get access token: {response.text}')
        else:
            print('Using cached access token')
            return cache['auth']['access_token']
    except Exception as e:
        print(f'Error getting access token: {e}')

def trigger_pipeline(access_token, project_name, pipeline_name, environment_name, scalar_variables):
    # Prepare the API request payload
    matillion_payload = {
        'pipelineName': pipeline_name,
        'environmentName': environment_name,
        'scalarVariables': scalar_variables
    }

    project_id = DPC_PROJECT_NAME_TO_ID_MAP.get(project_name)
    if not project_id:
        raise Exception(f'Project ID not found for project: {project_name}')


    # Define the Matillion DPC API endpoints
    api_url = f'https://{MATILLION_REGION}.api.matillion.com/dpc/v1/projects/{project_id}/pipeline-executions'

    # Send the request to Matillion DPC to trigger the pipeline
    print (f'Requesting execution of {pipeline_name}')
    response = requests.post(
        api_url,
        headers={
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        },
        data=json.dumps(matillion_payload)
    )

    # Check the response from the Matillion DPC API
    if response.status_code == 201:
        print(f'Pipeline {pipeline_name} triggered successfully')
        return response.text
    else:
        print(f'Failed to trigger pipeline {pipeline_name}. Response: {response.text}')
        raise Exception(f'Failed to trigger pipeline {pipeline_name}. Response: {response.text}')

def lambda_handler(event, context):
    # Loop through the SQS messages (FIFO queues can send batches of messages)
    recordLog = []

    for record in event['Records']:
        # Parse the incoming SQS message
        print('Get the SQS Message')
        sqs_message = json.loads(record['body'])

        # Extract details from the SQS message
        print('')
        project_name = sqs_message['projectName']
        environment_name = sqs_message['environmentName']
        pipeline_name = sqs_message['pipelineName']
        scalar_variables = sqs_message.get('scalarVariables', {})

        # Get an access token for the Matillion DPC API
        access_token = get_matillion_access_token()

        # Trigger the pipeline execution
        try:
            print('Attempt to trigger pipeline execution')
            response = trigger_pipeline(access_token, project_name, pipeline_name, environment_name, scalar_variables)
            recordLog.append({'statusCode': 200, 'message': f'Triggered {pipeline_name} successfully! {response}'})
        except Exception as e:
            print(f'Error triggering pipeline!: {e}')
            recordLog.append({'statusCode': 500, 'message': f'Error triggering pipeline: {e}'})

    has_err = any(item.get('statusCode') == 500 for item in recordLog)

    return {
        'statusCode': 500 if has_err else 200,
        'body': recordLog
    }
