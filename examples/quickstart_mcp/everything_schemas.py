# ruff: noqa: E501
"""Generated MCP tool schemas for the `everything` server.

DO NOT EDIT BY HAND. Re-run `calfkit mcp codegen` when the upstream MCP
server changes; the auto-generated content here will be overwritten.

Source: stdio: npx -y @modelcontextprotocol/server-everything
Generated: 2026-05-31T01:36:55Z
Tool count: 13
"""

from __future__ import annotations

from calfkit.mcp import McpToolDef

ECHO = McpToolDef(
    name="echo",
    description="Echoes back the input string",
    input_schema={
        "type": "object",
        "properties": {
            "message": {
                "type": "string",
                "description": "Message to echo",
            },
        },
        "required": [
            "message",
        ],
        "additionalProperties": False,
        "$schema": "http://json-schema.org/draft-07/schema#",
    },
    title="Echo Tool",
)

GET_ANNOTATED_MESSAGE = McpToolDef(
    name="get-annotated-message",
    description="Demonstrates how annotations can be used to provide metadata about content.",
    input_schema={
        "type": "object",
        "properties": {
            "messageType": {
                "type": "string",
                "enum": [
                    "error",
                    "success",
                    "debug",
                ],
                "description": "Type of message to demonstrate different annotation patterns",
            },
            "includeImage": {
                "type": "boolean",
                "default": False,
                "description": "Whether to include an example image",
            },
        },
        "required": [
            "messageType",
        ],
        "additionalProperties": False,
        "$schema": "http://json-schema.org/draft-07/schema#",
    },
    title="Get Annotated Message Tool",
)

GET_ENV = McpToolDef(
    name="get-env",
    description="Returns all environment variables, helpful for debugging MCP server configuration",
    input_schema={
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {},
    },
    title="Print Environment Tool",
)

GET_RESOURCE_LINKS = McpToolDef(
    name="get-resource-links",
    description="Returns up to ten resource links that reference different types of resources",
    input_schema={
        "type": "object",
        "properties": {
            "count": {
                "type": "number",
                "minimum": 1,
                "maximum": 10,
                "default": 3,
                "description": "Number of resource links to return (1-10)",
            },
        },
        "additionalProperties": False,
        "$schema": "http://json-schema.org/draft-07/schema#",
    },
    title="Get Resource Links Tool",
)

GET_RESOURCE_REFERENCE = McpToolDef(
    name="get-resource-reference",
    description="Returns a resource reference that can be used by MCP clients",
    input_schema={
        "type": "object",
        "properties": {
            "resourceType": {
                "type": "string",
                "enum": [
                    "Text",
                    "Blob",
                ],
                "default": "Text",
            },
            "resourceId": {
                "type": "number",
                "default": 1,
                "description": "ID of the text resource to fetch",
            },
        },
        "additionalProperties": False,
        "$schema": "http://json-schema.org/draft-07/schema#",
    },
    title="Get Resource Reference Tool",
)

GET_STRUCTURED_CONTENT = McpToolDef(
    name="get-structured-content",
    description="Returns structured content along with an output schema for client data validation",
    input_schema={
        "type": "object",
        "properties": {
            "location": {
                "type": "string",
                "enum": [
                    "New York",
                    "Chicago",
                    "Los Angeles",
                ],
                "description": "Choose city",
            },
        },
        "required": [
            "location",
        ],
        "additionalProperties": False,
        "$schema": "http://json-schema.org/draft-07/schema#",
    },
    output_schema={
        "type": "object",
        "properties": {
            "temperature": {
                "type": "number",
                "description": "Temperature in celsius",
            },
            "conditions": {
                "type": "string",
                "description": "Weather conditions description",
            },
            "humidity": {
                "type": "number",
                "description": "Humidity percentage",
            },
        },
        "required": [
            "temperature",
            "conditions",
            "humidity",
        ],
        "additionalProperties": False,
        "$schema": "http://json-schema.org/draft-07/schema#",
    },
    title="Get Structured Content Tool",
)

GET_SUM = McpToolDef(
    name="get-sum",
    description="Returns the sum of two numbers",
    input_schema={
        "type": "object",
        "properties": {
            "a": {
                "type": "number",
                "description": "First number",
            },
            "b": {
                "type": "number",
                "description": "Second number",
            },
        },
        "required": [
            "a",
            "b",
        ],
        "additionalProperties": False,
        "$schema": "http://json-schema.org/draft-07/schema#",
    },
    title="Get Sum Tool",
)

GET_TINY_IMAGE = McpToolDef(
    name="get-tiny-image",
    description="Returns a tiny MCP logo image.",
    input_schema={
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {},
    },
    title="Get Tiny Image Tool",
)

GZIP_FILE_AS_RESOURCE = McpToolDef(
    name="gzip-file-as-resource",
    description="Compresses a single file using gzip compression. Depending upon the selected output type, returns either the compressed data as a gzipped resource or a resource link, allowing it to be downloaded in a subsequent request during the current session.",
    input_schema={
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Name of the output file",
                "default": "README.md.gz",
            },
            "data": {
                "type": "string",
                "format": "uri",
                "description": "URL or data URI of the file content to compress",
                "default": "https://raw.githubusercontent.com/modelcontextprotocol/servers/refs/heads/main/README.md",
            },
            "outputType": {
                "type": "string",
                "enum": [
                    "resourceLink",
                    "resource",
                ],
                "default": "resourceLink",
                "description": "How the resulting gzipped file should be returned. 'resourceLink' returns a link to a resource that can be read later, 'resource' returns a full resource object.",
            },
        },
        "additionalProperties": False,
        "$schema": "http://json-schema.org/draft-07/schema#",
    },
    title="GZip File as Resource Tool",
)

SIMULATE_RESEARCH_QUERY = McpToolDef(
    name="simulate-research-query",
    description="Simulates a deep research operation that gathers, analyzes, and synthesizes information. Demonstrates MCP task-based operations with progress through multiple stages. If 'ambiguous' is true and client supports elicitation, sends an elicitation request for clarification.",
    input_schema={
        "type": "object",
        "properties": {
            "topic": {
                "type": "string",
                "description": "The research topic to investigate",
            },
            "ambiguous": {
                "type": "boolean",
                "default": False,
                "description": "Simulate an ambiguous query that requires clarification (triggers input_required status)",
            },
        },
        "required": [
            "topic",
        ],
        "additionalProperties": False,
        "$schema": "http://json-schema.org/draft-07/schema#",
    },
    title="Simulate Research Query",
)

TOGGLE_SIMULATED_LOGGING = McpToolDef(
    name="toggle-simulated-logging",
    description="Toggles simulated, random-leveled logging on or off.",
    input_schema={
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {},
    },
    title="Toggle Simulated Logging",
)

TOGGLE_SUBSCRIBER_UPDATES = McpToolDef(
    name="toggle-subscriber-updates",
    description="Toggles simulated resource subscription updates on or off.",
    input_schema={
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {},
    },
    title="Toggle Subscriber Updates",
)

TRIGGER_LONG_RUNNING_OPERATION = McpToolDef(
    name="trigger-long-running-operation",
    description="Demonstrates a long running operation with progress updates.",
    input_schema={
        "type": "object",
        "properties": {
            "duration": {
                "type": "number",
                "default": 10,
                "description": "Duration of the operation in seconds",
            },
            "steps": {
                "type": "number",
                "default": 5,
                "description": "Number of steps in the operation",
            },
        },
        "additionalProperties": False,
        "$schema": "http://json-schema.org/draft-07/schema#",
    },
    title="Trigger Long Running Operation Tool",
)


class Everything:
    """All tools exposed by the `everything` MCP server."""

    ECHO = ECHO
    GET_ANNOTATED_MESSAGE = GET_ANNOTATED_MESSAGE
    GET_ENV = GET_ENV
    GET_RESOURCE_LINKS = GET_RESOURCE_LINKS
    GET_RESOURCE_REFERENCE = GET_RESOURCE_REFERENCE
    GET_STRUCTURED_CONTENT = GET_STRUCTURED_CONTENT
    GET_SUM = GET_SUM
    GET_TINY_IMAGE = GET_TINY_IMAGE
    GZIP_FILE_AS_RESOURCE = GZIP_FILE_AS_RESOURCE
    SIMULATE_RESEARCH_QUERY = SIMULATE_RESEARCH_QUERY
    TOGGLE_SIMULATED_LOGGING = TOGGLE_SIMULATED_LOGGING
    TOGGLE_SUBSCRIBER_UPDATES = TOGGLE_SUBSCRIBER_UPDATES
    TRIGGER_LONG_RUNNING_OPERATION = TRIGGER_LONG_RUNNING_OPERATION

    ALL: list[McpToolDef] = [
        ECHO,
        GET_ANNOTATED_MESSAGE,
        GET_ENV,
        GET_RESOURCE_LINKS,
        GET_RESOURCE_REFERENCE,
        GET_STRUCTURED_CONTENT,
        GET_SUM,
        GET_TINY_IMAGE,
        GZIP_FILE_AS_RESOURCE,
        SIMULATE_RESEARCH_QUERY,
        TOGGLE_SIMULATED_LOGGING,
        TOGGLE_SUBSCRIBER_UPDATES,
        TRIGGER_LONG_RUNNING_OPERATION,
    ]
