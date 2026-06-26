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


async def test_gremlin_write_one_sample():
    gremlin_query = """
g.V().has('Application', 'id', 'app_repayment_predictor').fold().
  coalesce(
    unfold(),
    addV('Application').
      property('id', 'app_repayment_predictor').
      property('name', 'repayment_predictor').
      property('type', 'AI_MODEL').
      property('domain', 'Home_Lending')
  ).as('app').
V().has('DataAsset', 'id', 'asset_teradata_dw_gold_nps_summary_mart').fold().
  coalesce(
    unfold(),
    addV('DataAsset').
      property('id', 'asset_teradata_dw_gold_nps_summary_mart').
      property('platform', 'Teradata').
      property('database_name', 'DW_GOLD').
      property('table_name', 'nps_summary_mart').
      property('qualified_name', 'Teradata.DW_GOLD.nps_summary_mart').
      property('zone', 'GOLD')
  ).as('asset').
coalesce(
  __.select('app').outE('CONSUMES').where(inV().has('id', 'asset_teradata_dw_gold_nps_summary_mart')),
  __.select('app').addE('CONSUMES').to('asset')
).
property('source', 'Teradata DBQL').
property('service_account', 'svc_ai_runner').
property('query_count', 1).
property('first_seen', '2025-01-01T08:14:24Z').
property('last_seen', '2025-01-01T08:14:24Z').
property('confidence', 0.95).
property('batch_id', 'td-dbql-poc-001').
property('chunk_id', 'td-dbql-poc-001-chunk-001')
"""

    async with lambda_function_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            result = await session.call_tool(
                "run_gremlin_query",
                {
                    "query": gremlin_query
                },
            )

            return {
                "connected": True,
                "mcp_lambda": FUNCTION_NAME,
                "tool": "run_gremlin_query",
                "test": "write_one_sample_consumption_fact",
                "result": str(result),
            }


def lambda_handler(event, context):
    try:
        test_name = event.get("test", "list_tools")

        if test_name == "gremlin_write_one_sample":
            result = asyncio.run(test_gremlin_write_one_sample())
        else:
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