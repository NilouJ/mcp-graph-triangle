import os
import json
import asyncio
import traceback

from mcp import ClientSession
from mcp_lambda import LambdaFunctionParameters, lambda_function_client


REGION = os.environ.get("AWS_REGION", "ap-southeast-2")
FUNCTION_NAME = os.environ["MCP_LAMBDA_NAME"]

os.environ["AWS_REGION"] = REGION
os.environ["AWS_DEFAULT_REGION"] = REGION

server_params = LambdaFunctionParameters(
    function_name=FUNCTION_NAME,
    region_name=REGION,
)


def flatten_exception(exc):
    if isinstance(exc, BaseExceptionGroup):
        return {
            "type": type(exc).__name__,
            "message": str(exc),
            "sub_exceptions": [flatten_exception(e) for e in exc.exceptions],
        }

    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "traceback": "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ),
    }


async def test_mcp_connection():
    async with lambda_function_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            tools_result = await session.list_tools()

            tools = []
            for t in tools_result.tools:
                tools.append(
                    {
                        "name": t.name,
                        "description": getattr(t, "description", None),
                        "inputSchema": getattr(t, "inputSchema", None),
                    }
                )

            return {
                "connected": True,
                "mcp_lambda": FUNCTION_NAME,
                "tool_count": len(tools),
                "tools": tools,
            }


def lambda_handler(event, context):
    try:
        result = asyncio.run(test_mcp_connection())

        print(json.dumps(result, indent=2, default=str))

        return {
            "statusCode": 200,
            "body": json.dumps(result, default=str),
        }

    except BaseException as e:
        error = flatten_exception(e)

        print("ERROR:")
        print(json.dumps(error, indent=2, default=str))

        return {
            "statusCode": 500,
            "body": json.dumps(error, default=str),
        }