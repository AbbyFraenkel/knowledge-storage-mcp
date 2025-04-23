import uuid
import logging
from typing import Dict, Any, List, Optional

# MCP SDK imports
from modelcontextprotocol import MCPServer, MCPFunction, MCPFunctionParameter

# Local imports
from knowledge_storage_mcp.db.connection import Neo4jConnection
from knowledge_storage_mcp.db.schema import SchemaManager
from knowledge_storage_mcp.utils.logging import setup_logging

# Setup logging
logger = setup_logging(__name__)

# Default page size for list operations
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100

def register_entity_endpoints(server: MCPServer, db_connection: Neo4jConnection) -> None:
    """
    Register entity API endpoints with the MCP server.
    
    Args:
        server (MCPServer): The MCP server instance
        db_connection (Neo4jConnection): Database connection instance
    """
    logger.info("Registering entity API endpoints")
    schema_manager = SchemaManager(db_connection)
    
    @server.register_function(
        name="create_entity",
        description="Create a new entity in the knowledge graph",
        parameters=[
            MCPFunctionParameter(
                name="entity_type",
                description="Type of entity (e.g., 'Concept', 'Symbol')",
                required=True
            ),
            MCPFunctionParameter(
                name="properties",
                description="Entity properties following the schema for the entity type",
                required=True
            ),
            MCPFunctionParameter(
                name="provenance",
                description="Source information and creation metadata",
                required=False
            )
        ]
    )
    async def create_entity(entity_type: str, properties: Dict[str, Any],
                           provenance: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create a new entity in the knowledge graph.
        
        Args:
            entity_type (str): Type of entity (e.g., 'Concept', 'Symbol')
            properties (Dict[str, Any]): Entity properties
            provenance (Optional[Dict[str, Any]]): Source information and creation metadata
        
        Returns:
            Dict[str, Any]: Created entity information
        """
        logger.info(f"Creating entity of type '{entity_type}'")
        
        try:
            # Generate ID if not provided
            if "id" not in properties:
                properties["id"] = str(uuid.uuid4())
            
            # Validate properties against schema
            is_valid, errors = schema_manager.validate_entity(entity_type, properties)
            if not is_valid:
                return {
                    "success": False,
                    "errors": errors,
                    "message": "Entity validation failed"
                }
            
            # Add provenance if provided
            entity_props = {**properties}
            if provenance:
                entity_props["provenance"] = provenance
            
            # Build property string for Cypher query
            props_string = ", ".join([f"{k}: ${k}" for k in entity_props.keys()])
            
            # Create query with dynamic labels (Entity and the specific type)
            query = f"""
            CREATE (e:Entity:{entity_type} {{{props_string}}})
            RETURN e
            """
            
            # Execute query
            result = db_connection.execute_write_query(query, entity_props)
            
            return {
                "success": True,
                "entity_id": properties["id"],
                "entity_type": entity_type,
                "properties": properties,
                "message": "Entity created successfully"
            }
        except Exception as e:
            logger.error(f"Failed to create entity: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to create entity: {str(e)}"
            }
    
    @server.register_function(
        name="get_entity",
        description="Retrieve an entity by ID",
        parameters=[
            MCPFunctionParameter(
                name="entity_id",
                description="Entity identifier",
                required=True
            )
        ]
    )
    async def get_entity(entity_id: str) -> Dict[str, Any]:
        """
        Retrieve an entity by ID.
        
        Args:
            entity_id (str): Entity identifier
        
        Returns:
            Dict[str, Any]: Entity details or error message
        """
        logger.info(f"Retrieving entity with ID '{entity_id}'")
        
        try:
            # Query to find entity by ID
            query = """
            MATCH (e:Entity {id: $id})
            RETURN e
            """
            
            # Execute query
            result = db_connection.execute_query(query, {"id": entity_id})
            
            if not result:
                return {
                    "success": False,
                    "message": f"Entity with ID '{entity_id}' not found"
                }
            
            # Extract entity data
            entity_data = result[0]["e"]
            
            # Extract entity type (labels)
            entity_type = None
            query_labels = """
            MATCH (e:Entity {id: $id})
            RETURN labels(e) AS labels
            """
            labels_result = db_connection.execute_query(query_labels, {"id": entity_id})
            if labels_result and "labels" in labels_result[0]:
                labels = labels_result[0]["labels"]
                # Filter out 'Entity' to get the specific type
                entity_types = [label for label in labels if label != "Entity"]
                if entity_types:
                    entity_type = entity_types[0]
            
            return {
                "success": True,
                "entity": entity_data,
                "entity_type": entity_type,
                "entity_id": entity_id
            }
        except Exception as e:
            logger.error(f"Failed to retrieve entity: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to retrieve entity: {str(e)}"
            }
    
    @server.register_function(
        name="update_entity",
        description="Update an existing entity",
        parameters=[
            MCPFunctionParameter(
                name="entity_id",
                description="Entity identifier",
                required=True
            ),
            MCPFunctionParameter(
                name="properties",
                description="Updated entity properties",
                required=True
            )
        ]
    )
    async def update_entity(entity_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing entity with new properties.
        
        Args:
            entity_id (str): Entity identifier
            properties (Dict[str, Any]): Updated entity properties
        
        Returns:
            Dict[str, Any]: Updated entity information or error message
        """
        logger.info(f"Updating entity with ID '{entity_id}'")
        
        try:
            # First, check if entity exists and get its type
            query_entity = """
            MATCH (e:Entity {id: $id})
            RETURN labels(e) AS labels
            """
            result = db_connection.execute_query(query_entity, {"id": entity_id})
            
            if not result:
                return {
                    "success": False,
                    "message": f"Entity with ID '{entity_id}' not found"
                }
            
            # Extract entity type from labels
            labels = result[0]["labels"]
            entity_types = [label for label in labels if label != "Entity"]
            
            if not entity_types:
                return {
                    "success": False,
                    "message": "Entity has no type label"
                }
            
            entity_type = entity_types[0]
            
            # Cannot update id
            if "id" in properties and properties["id"] != entity_id:
                return {
                    "success": False,
                    "message": "Cannot change entity ID"
                }
            
            # Ensure ID is in properties for validation
            validated_props = {**properties, "id": entity_id}
            
            # Validate updated properties
            is_valid, errors = schema_manager.validate_entity(entity_type, validated_props)
            if not is_valid:
                return {
                    "success": False,
                    "errors": errors,
                    "message": "Entity validation failed"
                }
            
            # Build SET clause for Cypher query
            set_clauses = [f"e.{key} = ${key}" for key in properties.keys()]
            set_clause = ", ".join(set_clauses)
            
            # Update entity
            query = f"""
            MATCH (e:Entity {{id: $id}})
            SET {set_clause}
            RETURN e
            """
            
            # Add id to properties for query parameters
            query_params = {**properties, "id": entity_id}
            
            # Execute query
            result = db_connection.execute_write_query(query, query_params)
            
            return {
                "success": True,
                "entity_id": entity_id,
                "entity_type": entity_type,
                "updated_properties": properties,
                "message": "Entity updated successfully"
            }
        except Exception as e:
            logger.error(f"Failed to update entity: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to update entity: {str(e)}"
            }
    
    @server.register_function(
        name="delete_entity",
        description="Delete an entity from the knowledge graph",
        parameters=[
            MCPFunctionParameter(
                name="entity_id",
                description="Entity identifier",
                required=True
            ),
            MCPFunctionParameter(
                name="delete_relationships",
                description="Whether to delete all relationships to this entity",
                required=False
            )
        ]
    )
    async def delete_entity(entity_id: str, delete_relationships: bool = True) -> Dict[str, Any]:
        """
        Delete an entity from the knowledge graph.
        
        Args:
            entity_id (str): Entity identifier
            delete_relationships (bool): Whether to delete all relationships to this entity
        
        Returns:
            Dict[str, Any]: Deletion result or error message
        """
        logger.info(f"Deleting entity with ID '{entity_id}'")
        
        try:
            # Check if entity exists
            query_check = """
            MATCH (e:Entity {id: $id})
            RETURN e
            """
            result = db_connection.execute_query(query_check, {"id": entity_id})
            
            if not result:
                return {
                    "success": False,
                    "message": f"Entity with ID '{entity_id}' not found"
                }
            
            # Delete query based on delete_relationships parameter
            if delete_relationships:
                query = """
                MATCH (e:Entity {id: $id})
                OPTIONAL MATCH (e)-[r]->()
                OPTIONAL MATCH ()-[s]->(e)
                DELETE r, s, e
                """
            else:
                # Check if entity has relationships
                query_check_rels = """
                MATCH (e:Entity {id: $id})
                OPTIONAL MATCH (e)-[r]-()
                RETURN count(r) AS rel_count
                """
                rels_result = db_connection.execute_query(query_check_rels, {"id": entity_id})
                
                if rels_result and rels_result[0]["rel_count"] > 0:
                    return {
                        "success": False,
                        "message": f"Entity has relationships. Set delete_relationships=true to delete anyway."
                    }
                
                query = """
                MATCH (e:Entity {id: $id})
                DELETE e
                """
            
            # Execute delete query
            db_connection.execute_write_query(query, {"id": entity_id})
            
            return {
                "success": True,
                "entity_id": entity_id,
                "message": "Entity deleted successfully"
            }
        except Exception as e:
            logger.error(f"Failed to delete entity: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to delete entity: {str(e)}"
            }
    
    @server.register_function(
        name="list_entities",
        description="List entities from the knowledge graph with filtering and pagination",
        parameters=[
            MCPFunctionParameter(
                name="entity_type",
                description="Filter by entity type",
                required=False
            ),
            MCPFunctionParameter(
                name="properties",
                description="Filter by property values",
                required=False
            ),
            MCPFunctionParameter(
                name="page",
                description="Page number (0-based)",
                required=False
            ),
            MCPFunctionParameter(
                name="page_size",
                description="Number of results per page",
                required=False
            )
        ]
    )
    async def list_entities(entity_type: Optional[str] = None, 
                           properties: Optional[Dict[str, Any]] = None,
                           page: Optional[int] = 0, 
                           page_size: Optional[int] = DEFAULT_PAGE_SIZE) -> Dict[str, Any]:
        """
        List entities from the knowledge graph with filtering and pagination.
        
        Args:
            entity_type (Optional[str]): Filter by entity type
            properties (Optional[Dict[str, Any]]): Filter by property values
            page (Optional[int]): Page number (0-based)
            page_size (Optional[int]): Number of results per page
        
        Returns:
            Dict[str, Any]: List of entities matching the filters
        """
        logger.info(f"Listing entities with type='{entity_type}', page={page}, page_size={page_size}")
        
        try:
            # Validate pagination parameters
            if page < 0:
                page = 0
            if page_size <= 0 or page_size > MAX_PAGE_SIZE:
                page_size = DEFAULT_PAGE_SIZE
            
            # Calculate skip value for pagination
            skip = page * page_size
            
            # Start building the query
            query_parts = ["MATCH (e:Entity"]
            params = {}
            
            # Add entity type filter if provided
            if entity_type:
                query_parts[0] += f":{entity_type}"
            
            query_parts[0] += ")"  # Close the MATCH clause
            
            # Add property filters if provided
            if properties:
                where_clauses = []
                for key, value in properties.items():
                    param_key = f"prop_{key}"
                    where_clauses.append(f"e.{key} = ${param_key}")
                    params[param_key] = value
                
                if where_clauses:
                    query_parts.append("WHERE " + " AND ".join(where_clauses))
            
            # Add pagination
            count_query = " ".join(query_parts + ["RETURN count(e) AS count"])
            
            # Add return, skip, and limit
            query_parts.append(f"RETURN e SKIP {skip} LIMIT {page_size}")
            
            # Build the final query
            query = " ".join(query_parts)
            
            # Get total count for pagination
            count_result = db_connection.execute_query(count_query, params)
            total_count = count_result[0]["count"] if count_result else 0
            
            # Execute the main query
            result = db_connection.execute_query(query, params)
            
            # Extract entities from result
            entities = []
            for record in result:
                entity = record["e"]
                entities.append(entity)
            
            # Calculate pagination metadata
            total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
            has_next = page < total_pages - 1
            has_prev = page > 0
            
            return {
                "success": True,
                "entities": entities,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_count": total_count,
                    "total_pages": total_pages,
                    "has_next": has_next,
                    "has_prev": has_prev
                }
            }
        except Exception as e:
            logger.error(f"Failed to list entities: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to list entities: {str(e)}"
            }
    
    @server.register_function(
        name="get_entity_by_properties",
        description="Find an entity by its properties",
        parameters=[
            MCPFunctionParameter(
                name="entity_type",
                description="Entity type",
                required=False
            ),
            MCPFunctionParameter(
                name="properties",
                description="Property values to match",
                required=True
            )
        ]
    )
    async def get_entity_by_properties(properties: Dict[str, Any], 
                                      entity_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Find an entity by matching property values.
        
        Args:
            properties (Dict[str, Any]): Property values to match
            entity_type (Optional[str]): Entity type to filter by
        
        Returns:
            Dict[str, Any]: Entity details or error message
        """
        logger.info(f"Finding entity by properties: {properties}")
        
        try:
            # Start building the query
            query_parts = ["MATCH (e:Entity"]
            params = {}
            
            # Add entity type filter if provided
            if entity_type:
                query_parts[0] += f":{entity_type}"
            
            query_parts[0] += ")"  # Close the MATCH clause
            
            # Add property filters
            where_clauses = []
            for key, value in properties.items():
                param_key = f"prop_{key}"
                where_clauses.append(f"e.{key} = ${param_key}")
                params[param_key] = value
            
            if where_clauses:
                query_parts.append("WHERE " + " AND ".join(where_clauses))
            
            # Add return and limit to 1
            query_parts.append("RETURN e LIMIT 1")
            
            # Build the final query
            query = " ".join(query_parts)
            
            # Execute the query
            result = db_connection.execute_query(query, params)
            
            if not result:
                return {
                    "success": False,
                    "message": "No entity found matching the specified properties"
                }
            
            # Extract entity from result
            entity = result[0]["e"]
            
            return {
                "success": True,
                "entity": entity,
                "entity_id": entity.get("id")
            }
        except Exception as e:
            logger.error(f"Failed to find entity by properties: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to find entity by properties: {str(e)}"
            }
    
    logger.info("Entity API endpoints registered")