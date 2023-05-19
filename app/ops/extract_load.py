from typing import Dict, List

import boto3
import psycopg2.extras as p
import requests
from dagster import op
from utils.config import (
    get_aws_creds,
    get_customer_db_creds,
    get_warehouse_creds,
)
from utils.db import WarehouseConnection

#<-------------- Customer Risk Score ------------------>
@op(config_schema={"risk_endpoint": str})
def extract_customer_risk_score(context) -> List[Dict[str, int]]:
    pass

@op
def load_customer_risk_score(customer_risk_score: List[Dict[str, int]]):
    pass

#<-------------- orders data ------------------>
# extract orders data
@op(config_schema={"orders_bucket_name": str})
def extract_orders_data(context) -> List[Dict[str, str]]:
    pass

@op
def load_orders_data(orders_data: List[Dict[str, str]]):
    pass

#<-------------- Customer Data ------------------>
@op
def extract_customer_data() -> List[Dict[str, str]]:
    pass

@op
def load_customer_data(customers_data: List[Dict[str, str]]):
    pass    