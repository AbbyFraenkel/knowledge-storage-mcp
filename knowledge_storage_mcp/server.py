"""
Knowledge Storage MCP Server

This is the main Model Context Protocol server implementation for the Knowledge Storage MCP.
It provides endpoints for entity and relationship management in the knowledge graph.
"""

import os
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, HTTPException
from modelcontextprotocol.server import MCPServer
from modelcontextprotocol.schema import MCPTool, MCPToolParameter, MCPToolResponse
from loguru import logger

from knowledge_storage_mcp.db.neo4j_client import Neo4jClient
from knowledge_storage_mcp.config import get_settings

settings = get_settings()

# Initialize FastAPI app
app = FastAPI(
    title="Knowledge Storage MCP",
    description="Model Context Protocol server for academic knowledge storage and retrieval",
    version="0.1.0"
)

# Initialize Neo4j client
neo4j_client = Neo4jClient(
    uri=settings.neo4j_uri,
    user=settings.neo4j_user,
    password=settings.neo4j_password
)

# Define MCP tools
create_entity_tool = MCPTool(
    name="create_entity",
    description="Create a knowledge entity with properties and provenance",
    parameters=[
        MCPToolParameter(name="entity_type", type="string"),
        MCPToolParameter(name="properties", type="object"),
        MCPToolParameter(name="provenance", type="object", required=False)
    ]
)

create_relationship_tool = MCPTool(
    name="create_relationship",
    description="Create a relationship between two entities",
    parameters=[
        MCPToolParameter(name="from_entity_id", type="string"),
        MCPToolParameter(name="relationship_type", type="string"),
        MCPToolParameter(name="to_entity_id", type="string"),
        MCPToolParameter(name="properties", type="object", required=False)
    ]
)

query_knowledge_graph_tool = MCPTool(
    name="query_knowledge_graph",
    description="Query the knowledge graph based on parameters",
    parameters=[
        MCPToolParameter(name="query_params", type="object"),
        MCPToolParameter(name="output_format", type="string", required=False)
    ]
)

# Initialize MCP server
mcp_server = MCPServer(
    tools=[
        create_entity_tool,
        create_relationship_tool,
        query_knowledge_graph_tool
    ]
)

# Register MCP handlers
@mcp_server.register_tool("create_entity")
async def handle_create_entity(
    entity_type: str,
    properties: Dict[str, Any],
    provenance: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Create a new entity in the knowledge graph.
    
    Args:
        entity_type: Type of entity (e.g., 'Document', 'Concept', 'Algorithm', 'Symbol')
        properties: Entity properties following the schema for the entity type
        provenance: Optional source information and creation metadata
        
    Returns:
        Dict containing the created entity information
    """
    try:
        # Validate entity_type against schema
        # This is a placeholder - actual implementation would validate against schema
        if not entity_type:
            raise ValueError("Entity type cannot be empty")
            
        # Create entity in Neo4j
        entity_id = neo4j_client.create_entity(
            entity_type=entity_type,
            properties=properties,
            provenance=provenance or {}
        )
        
        return {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "properties": properties,
            "created_at": neo4j_client.get_timestamp(),
            "status": "created"
        }
    except Exception as e:
        logger.error(f"Error creating entity: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create entity: {str(e)}")

@mcp_server.register_tool("create_relationship")
async def handle_create_relationship(
    from_entity_id: str,
    relationship_type: str,
    to_entity_id: str,
    properties: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Create a relationship between two entities in the knowledge graph.
    
    Args:
        from_entity_id: Source entity identifier
        relationship_type: Type of relationship (e.g., 'CONTAINS', 'IMPLEMENTS', 'REFERENCES')
        to_entity_id: Target entity identifier
        properties: Optional relationship properties
        
    Returns:
        Dict containing the created relationship information
    """
    try:
        # Validate entities exist
        if not neo4j_client.entity_exists(from_entity_id):
            raise ValueError(f"Source entity {from_entity_id} does not exist")
        
        if not neo4j_client.entity_exists(to_entity_id):
            raise ValueError(f"Target entity {to_entity_id} does not exist")
            
        # Create relationship in Neo4j
        relationship_id = neo4j_client.create_relationship(
            from_entity_id=from_entity_id,
            relationship_type=relationship_type,
            to_entity_id=to_entity_id,
            properties=properties or {}
        )
        
        return {
            "relationship_id": relationship_id,
            "from_entity_id": from_entity_id,
            "relationship_type": relationship_type,
            "to_entity_id": to_entity_id,
            "properties": properties or {},
            "created_at": neo4j_client.get_timestamp(),
            "status": "created"
        }
    except Exception as e:
        logger.error(f"Error creating relationship: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create relationship: {str(e)}")

@mcp_server.register_tool("query_knowledge_graph")
async def handle_query_knowledge_graph(
    query_params: Dict[str, Any],
    output_format: Optional[str] = None
) -> Dict[str, Any]:
    """
    Query the knowledge graph based on parameters.
    
    Args:
        query_params: Query parameters including entity_types, properties, relationships, etc.
        output_format: Optional desired output format (e.g., 'json', 'cypher', 'xml')
        
    Returns:
        Dict containing the query results
    """
    try:
        # Execute query
        results = neo4j_client.query_knowledge_graph(
            query_params=query_params,
            output_format=output_format or "json"
        )
        
        return {
            "entities": results.get("entities", []),
            "relationships": results.get("relationships", []),
            "metadata": {
                "count": len(results.get("entities", [])),
                "query_time": results.get("query_time", 0),
                "format": output_format or "json"
            }
        }
    except Exception as e:
        logger.error(f"Error querying knowledge graph: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to query knowledge graph: {str(e)}")

# Register the MCP server with FastAPI
app.mount("/mcp", mcp_server)

@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "Knowledge Storage MCP"}

@app.get("/health")
async def health():
    """Health check endpoint with database connectivity test."""
    try:
        # Check database connectivity
        db_status = neo4j_client.check_connection()
        return {
            "status": "ok" if db_status else "error",
            "service": "Knowledge Storage MCP",
            "database": "connected" if db_status else "disconnected"
        }
    except Exception as e:
        return {
            "status": "error",
            "service": "Knowledge Storage MCP",
            "database": "disconnected",
            "error": str(e)
        }

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "knowledge_storage_mcp.server:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug
    )
