import os
import json
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

async def get_schema():
    async with lambda_function_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            tools_result = await session.list_tools()
            print("CONNECTED\n")

            for t in tools_result.tools:
                print("TOOL:", t.name)
                if getattr(t, "description", None):
                    print("  desc:", t.description)
                if getattr(t, "inputSchema", None):
                    print("  inputSchema:", t.inputSchema)
                print()

            schema_tool_name = "get_schema"

            result = await session.call_tool(schema_tool_name, {})
            print("\nSCHEMA RESULT:")
            print(result)

def lambda_handler(event, context):
    import asyncio
    asyncio.run(get_schema())