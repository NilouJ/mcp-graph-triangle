import os
import sys
import boto3

from mcp.client.stdio import StdioServerParameters
from mcp_lambda import stdio_server_adapter

REGION = os.environ.get("REGION", "ap-southeast-2")
NEPTUNE_HOST = os.environ["NEPTUNE_ENDPOINT"].strip()
NEPTUNE_PORT = os.environ.get("NEPTUNE_PORT", "8182").strip()
GRAPH_ENDPOINT = f"neptune-db://{NEPTUNE_HOST}:{NEPTUNE_PORT}"

session = boto3.Session()
creds = session.get_credentials()
if creds is None:
    raise RuntimeError("No Lambda credentials found")

frozen = creds.get_frozen_credentials()

server_params = StdioServerParameters(
    command=sys.executable,
    args=["-m", "awslabs.amazon_neptune_mcp_server.server"],
    env={
        "PYTHONPATH": "/var/task",
        "AWS_REGION": REGION,
        "AWS_DEFAULT_REGION": REGION,
        "AWS_ACCESS_KEY_ID": frozen.access_key,
        "AWS_SECRET_ACCESS_KEY": frozen.secret_key,
        "AWS_SESSION_TOKEN": frozen.token or "",
        "NEPTUNE_ENDPOINT": GRAPH_ENDPOINT,
        "FASTMCP_LOG_LEVEL": "INFO",
    },
)

def lambda_handler(event, context):
    return stdio_server_adapter(server_params, event, context)