import random
import time
import uuid
from datetime import datetime
from typing import Any,List, Tuple

import boto3
from faker import Faker

from datagenerator.utils.db import DatabaseConnection
from datagenerator.utils.config import get_database_creds

import configparser

# import aws configs
config = configparser.ConfigParser()
config.read_file(open('cluster.config'))

URL = config.get('AWS', 'URL')
KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')
REGION = config.get('AWS','REGION')

STATES_LIST = [
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
]

def _get_orders(cust_ids: List[int], num_orders: int) ->str:
    # order_id, customer_id, item_id, item_name, delivered_on
    items = [
        "chair",
        "car",
        "toy",
        "laptop",
        "box",
        "food",
        "shirt",
        "weights",
        "bags",
        "carts",
    ]
    data = ""
    for _ in range(num_orders):
        data += f'{uuid.uuid4()},{random.choice(cust_ids)},'
        data += f'{uuid.uuid4()},{random.choice(items)},'
        data += f'{datetime.now().strftime("%y-%m-%d %H:%M:%S")}'
        data += "\n"
        
    return data
    
def _get_customer_data(cust_ids: List[int], ) -> List[Tuple[int, Any, Any, str, str, str]]:
    fake = Faker()
    
    return [
        (
            cust_id,
            fake.first_name(),
            fake.last_name(),
            random.choice(STATES_LIST),
            datetime.now().strftime("%y-%m-%d %H:%M:%S"),
            datetime.now().strftime("%y-%m-%d %H:%M:%S"),
        )
        for cust_id in cust_ids
    ]

def _customer_data_insert_query() -> str:
    return """
    INSERT INTO customers (
        customer_id,
        first_name,
        last_name,
        state_code,
        datetime_created,
        datetime_updated
    )
    VALUES (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    )
    on conflict(customer_id)
    do update set
        (first_name, last_name, state_code, datetime_updated) = 
        (EXCLUDED.first_name, EXCLUDED.last_name, 
        EXLCUDED.state_code, EXCLUDED.datetime_created);
    """

def generate_data(iteration: int, orders_bucket: str = "app-orders") -> None:
    cust_ids = [random.randint(1,10000) for _ in range(1000)]
    orders_data = _get_orders(cust_ids, 1000)
    customer_data = _get_customer_data(cust_ids)
    
    # send the data to s3 bucket
    s3 = boto3.resource(
        's3',
        endpoint_url=URL,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=REGION
    )
    
    # Create bucket if not does not exist
    if not s3.Bucket(orders_bucket) in s3.buckets.all():
        s3.create_bucket(Bucket=orders_bucket)
    s3.Object(orders_bucket, f'data_{str(iteration)}.csv').put(Body=orders_data)
    
    # insert customers data to customer_db
    with DatabaseConnection(get_database_creds()).managed_cursor() as curr:
        insert_query = _customer_data_insert_query()
        for cd in customer_data:
            curr.execute(
                insert_query,
                         (
                            cd[0],
                            cd[1],
                            cd[2],
                            cd[3],
                            cd[4],
                            cd[5],
                         ),
                    )


if __name__ == '__main__':
    itr = 1
    while True:
        generate_data(itr)
        time.sleep(30)
        itr += 1