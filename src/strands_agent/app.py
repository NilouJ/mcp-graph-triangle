import os
import json
import asyncio
import traceback

from mcp import ClientSession
from mcp_lambda import LambdaFunctionParameters, lambda_function_client

from strands import Agent
from strands.models import BedrockModel
from strands.tools.mcp import MCPClient


REGION = os.environ.get("AWS_REGION", "ap-southeast-2")
FUNCTION_NAME = os.environ["MCP_LAMBDA_NAME"]
BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID",
    "anthropic.claude-3-haiku-20240307-v1:0",
)

os.environ["AWS_REGION"] = REGION
os.environ["AWS_DEFAULT_REGION"] = REGION

server_params = LambdaFunctionParameters(
    function_name=FUNCTION_NAME,
    region_name=REGION,
)


AGENT_SYSTEM_PROMPT = """
You are a Neptune graph assistant connected to an MCP server.

The MCP server exposes graph tools such as:
- get_graph_status
- get_graph_schema
- run_gremlin_query
- run_opencypher_query

Rules:
1. For graph questions, first inspect the graph schema using get_graph_schema.
2. Use run_gremlin_query or run_opencypher_query to answer the user question.
3. Do not invent graph contents.
4. Always mention which query/tool you used.
5. Keep the answer concise.
6. If the graph has no matching data, say that clearly.
"""


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


def serialize_mcp_result(result):
    output = {
        "raw": str(result),
        "is_error": getattr(result, "isError", None),
        "content": [],
        "structured_content": getattr(result, "structuredContent", None),
    }

    content_items = getattr(result, "content", None)
    if content_items:
        for item in content_items:
            output["content"].append(
                {
                    "type": getattr(item, "type", None),
                    "text": getattr(item, "text", None),
                }
            )

    return output


async def list_mcp_tools():
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


async def call_mcp_tool(tool_name, arguments=None):
    if arguments is None:
        arguments = {}

    async with lambda_function_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            result = await session.call_tool(tool_name, arguments)

            return {
                "connected": True,
                "mcp_lambda": FUNCTION_NAME,
                "tool": tool_name,
                "arguments": arguments,
                "result": serialize_mcp_result(result),
            }


async def run_gremlin_query(query):
    return await call_mcp_tool(
        "run_gremlin_query",
        {
            "query": query,
        },
    )


async def run_opencypher_query(query, parameters=None):
    return await call_mcp_tool(
        "run_opencypher_query",
        {
            "query": query,
            "parameters": parameters,
        },
    )


def run_strands_agent_with_mcp(question):
    model = BedrockModel(
        model_id=BEDROCK_MODEL_ID,
        region_name=REGION,
    )

    mcp_client = MCPClient(lambda: lambda_function_client(server_params))

    with mcp_client:
        tools = mcp_client.list_tools_sync()

        agent = Agent(
            model=model,
            tools=tools,
            system_prompt=AGENT_SYSTEM_PROMPT,
        )

        response = agent(question)

    return {
        "agent": "strands_agent_with_mcp_tools",
        "mcp_lambda": FUNCTION_NAME,
        "model_id": BEDROCK_MODEL_ID,
        "question": question,
        "response": str(response),
    }


def lambda_handler(event, context):
    try:
        print("EVENT:")
        print(json.dumps(event, indent=2, default=str))

        test_name = event.get("test", "list_tools")

        if test_name == "list_tools":
            result = asyncio.run(list_mcp_tools())

        elif test_name == "graph_status":
            result = asyncio.run(call_mcp_tool("get_graph_status"))

        elif test_name == "graph_schema":
            result = asyncio.run(call_mcp_tool("get_graph_schema"))

        elif test_name == "run_gremlin":
            query = event.get("query")

            if not query:
                return {
                    "statusCode": 400,
                    "body": json.dumps(
                        {
                            "error": "Missing required field: query",
                            "example": {
                                "test": "run_gremlin",
                                "query": "g.V().limit(10).valueMap(true)",
                            },
                        }
                    ),
                }

            result = asyncio.run(run_gremlin_query(query))

        elif test_name == "run_opencypher":
            query = event.get("query")
            parameters = event.get("parameters")

            if not query:
                return {
                    "statusCode": 400,
                    "body": json.dumps(
                        {
                            "error": "Missing required field: query",
                            "example": {
                                "test": "run_opencypher",
                                "query": "MATCH (n) RETURN n LIMIT 10",
                                "parameters": {},
                            },
                        }
                    ),
                }

            result = asyncio.run(run_opencypher_query(query, parameters))

        elif test_name == "call_tool":
            tool_name = event.get("tool")
            arguments = event.get("arguments", {})

            if not tool_name:
                return {
                    "statusCode": 400,
                    "body": json.dumps(
                        {
                            "error": "Missing required field: tool",
                            "example": {
                                "test": "call_tool",
                                "tool": "get_graph_status",
                                "arguments": {},
                            },
                        }
                    ),
                }

            result = asyncio.run(call_mcp_tool(tool_name, arguments))

        elif test_name == "agent_mcp":
            question = event.get(
                "question",
                "What data assets does app_repayment_predictor consume?",
            )

            result = run_strands_agent_with_mcp(question)

        else:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {
                        "error": f"Unknown test: {test_name}",
                        "allowed_tests": [
                            "list_tools",
                            "graph_status",
                            "graph_schema",
                            "run_gremlin",
                            "run_opencypher",
                            "call_tool",
                            "agent_mcp",
                        ],
                    }
                ),
            }

        print("RESULT:")
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