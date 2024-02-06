import argparse
import json
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

def fetch_snowflake_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    return json.loads(get_secret_value_response['SecretString'])

def make_snowflake_connection_dict(account, warehouse, database, schema, secrets_region_name="us-east-1"):
    secret_name = f'snowflake/{account}'
    connection_dict = fetch_snowflake_secret(secret_name, secrets_region_name)
    if warehouse:
        connection_dict['warehouse'] = warehouse
    connection_dict['database'] = database
    connection_dict['schema'] = schema
    return connection_dict

def create_snowflake_engine(account):
    connection_dict = make_snowflake_connection_dict(account=account, warehouse=None, database='', schema='')
    connection_string = 'snowflake://{user}:{password}@{account_identifier}/'
    engine = create_engine(
        connection_string.format(
            user=connection_dict['username'],
            password=connection_dict['password'],
            account_identifier=connection_dict['account_identifier'],
            warehouse=connection_dict['warehouse'],
        )
    )
    try:
        connection = engine.connect()
        results = connection.execute('select current_version()').fetchone()
        print(results[0])
    finally:
        connection.close()
        engine.dispose()
    return engine

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Create snowflake connection engine.')
    parser.add_argument('-a', '--account', required=True)
    parser.add_argument('-w', '--warehouse', required=False)
    parser.add_argument('-d', '--database', required=False)
    parser.add_argument('-s', '--schema', required=False)
    parser.add_argument('--keepalive', required=False, default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument('engine_key')
    args = parser.parse_args()
    connection_dict = make_snowflake_connection_dict(account=args.account, warehouse=args.warehouse, database=args.database, schema=args.schema)
    try:
        engine
    except NameError:
        engine = {}
    connection_string = 'snowflake://{user}:{password}@{account_identifier}/'
    format_args = {
        'user': connection_dict['username'],
        'password': connection_dict['password'],
        'account_identifier': connection_dict['account_identifier'],
        'warehouse': connection_dict['warehouse'],
    }
    if args.database is not None:
        connection_string += '{database}/'
        format_args['database'] = connection_dict['database']
        if args.schema is not None:
            connection_string += '{schema}'
            format_args['schema'] = connection_dict['schema']
    connection_string += '?warehouse={warehouse}'
    engine[args.engine_key] = create_engine(
        connection_string.format(**format_args),
        connect_args = {
            'client_session_keep_alive': True,
        }
    )

    try:
        connection = engine[args.engine_key].connect()
        results = connection.execute('select current_version()').fetchone()
        print(results[0])
    finally:
        connection.close()
        engine[args.engine_key].dispose()
