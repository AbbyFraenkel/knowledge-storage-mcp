"""
Neo4j database client for Knowledge Storage MCP.

This module provides a client for interacting with Neo4j database,
implementing all the required operations for the knowledge graph.
"""

import time
import uuid
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

from neo4j import GraphDatabase, Driver, Session
from loguru import logger


class Neo4jClient:
    """
    Neo4j client for Knowledge Storage MCP.
    
    This class handles all interactions with the Neo4j database, providing
    methods for entity and relationship operations, queries, and graph management.
    """
    
    def __init__(self, uri: str, user: str, password: str):
        """
        Initialize Neo4j client.
        
        Args:
            uri: Neo4j connection URI
            user: Neo4j username
            password: Neo4j password
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = self._create_driver()
        
    def _create_driver(self) -> Driver:
        """
        Create Neo4j driver instance.
        
        Returns:
            Neo4j driver instance
        """
        try:
            return GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        except Exception as e:
            logger.error(f"Failed to create Neo4j driver: {str(e)}")
            raise

    def check_connection(self) -> bool:
        """
        Check if connection to Neo4j is working.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1 AS result")
                return result.single()["result"] == 1
        except Exception as e:
            logger.error(f"Neo4j connection check failed: {str(e)}")
            return False
            
    def close(self):
        """Close the Neo4j driver connection."""
        if self.driver:
            self.driver.close()
            
    def get_timestamp(self) -> str:
        """
        Get current ISO timestamp.
        
        Returns:
            ISO formatted timestamp
        """
        return datetime.utcnow().isoformat() + "Z"
            
    def create_entity(
        self, 
        entity_type: str, 
        properties: Dict[str, Any], 
        provenance: Dict[str, Any]
    ) -> str:
        """
        Create a new entity in the knowledge graph.
        
        Args:
            entity_type: Type of entity (e.g., 'Document', 'Concept', 'Algorithm', 'Symbol')
            properties: Entity properties
            provenance: Source information and creation metadata
            
        Returns:
            Entity ID
        """
        # Generate a unique ID
        entity_id = str(uuid.uuid4())
        
        # Add metadata to properties
        entity_properties = {
            "id": entity_id,
            "type": entity_type,
            "created_at": self.get_timestamp(),
            "updated_at": self.get_timestamp(),
            **properties
        }
        
        # Add provenance as a property if provided
        if provenance:
            entity_properties["provenance"] = provenance
            
        # Create Cypher query
        query = f"""
        CREATE (e:{entity_type} $properties)
        RETURN e.id as id
        """
        
        # Execute query
        with self.driver.session() as session:
            result = session.run(query, parameters={"properties": entity_properties})
            return result.single()["id"]
            
    def entity_exists(self, entity_id: str) -> bool:
        """
        Check if an entity exists in the knowledge graph.
        
        Args:
            entity_id: Entity ID to check
            
        Returns:
            True if entity exists, False otherwise
        """
        query = """
        MATCH (e {id: $entity_id})
        RETURN count(e) as count
        """
        
        with self.driver.session() as session:
            result = session.run(query, parameters={"entity_id": entity_id})
            return result.single()["count"] > 0
            
    def create_relationship(
        self,
        from_entity_id: str,
        relationship_type: str,
        to_entity_id: str,
        properties: Dict[str, Any]
    ) -> str:
        """
        Create a relationship between two entities.
        
        Args:
            from_entity_id: Source entity ID
            relationship_type: Type of relationship
            to_entity_id: Target entity ID
            properties: Relationship properties
            
        Returns:
            Relationship ID
        """
        # Generate a unique ID
        relationship_id = str(uuid.uuid4())
        
        # Add metadata to properties
        relationship_properties = {
            "id": relationship_id,
            "created_at": self.get_timestamp(),
            **properties
        }
        
        # Create Cypher query
        query = f"""
        MATCH (from {{id: $from_entity_id}})
        MATCH (to {{id: $to_entity_id}})
        CREATE (from)-[r:{relationship_type} $properties]->(to)
        RETURN r.id as id
        """
        
        # Execute query
        with self.driver.session() as session:
            result = session.run(
                query, 
                parameters={
                    "from_entity_id": from_entity_id,
                    "to_entity_id": to_entity_id,
                    "properties": relationship_properties
                }
            )
            return result.single()["id"]
            
    def query_knowledge_graph(
        self,
        query_params: Dict[str, Any],
        output_format: str = "json"
    ) -> Dict[str, Any]:
        """
        Query the knowledge graph based on parameters.
        
        Args:
            query_params: Query parameters
            output_format: Output format
            
        Returns:
            Query results
        """
        start_time = time.time()
        
        # Extract query parameters
        entity_types = query_params.get("entity_types", [])
        properties = query_params.get("properties", {})
        relationships = query_params.get("relationships", [])
        filters = query_params.get("filters", {})
        pagination = query_params.get("pagination", {"skip": 0, "limit": 100})
        
        # Build Cypher query
        query_parts = []
        where_clauses = []
        return_clauses = ["e", "r", "related"]
        parameters = {}
        
        # Handle entity types
        if entity_types:
            entity_labels = ":".join(entity_types)
            query_parts.append(f"MATCH (e:{entity_labels})")
        else:
            query_parts.append("MATCH (e)")
            
        # Handle properties
        if properties:
            for key, value in properties.items():
                where_clauses.append(f"e.{key} = ${key}")
                parameters[key] = value
                
        # Handle filters
        if filters:
            for key, value in filters.items():
                if isinstance(value, dict):
                    # Handle operators like >, <, >=, <=
                    operator = value.get("operator", "=")
                    filter_value = value.get("value")
                    if filter_value is not None:
                        where_clauses.append(f"e.{key} {operator} ${key}")
                        parameters[key] = filter_value
                else:
                    where_clauses.append(f"e.{key} = ${key}")
                    parameters[key] = value
                    
        # Handle relationships
        if relationships:
            for i, rel in enumerate(relationships):
                rel_type = rel.get("type", "")
                direction = rel.get("direction", "outgoing")
                target_label = rel.get("target_type", "")
                
                # Build relationship pattern
                if direction == "outgoing":
                    pattern = f"(e)-[r{i}:{rel_type}]->(related{i}"
                elif direction == "incoming":
                    pattern = f"(e)<-[r{i}:{rel_type}]-(related{i}"
                else:  # bidirectional
                    pattern = f"(e)-[r{i}:{rel_type}]-(related{i}"
                    
                # Add target label if specified
                if target_label:
                    pattern += f":{target_label}"
                    
                pattern += ")"
                query_parts.append(f"OPTIONAL MATCH {pattern}")
                return_clauses.append(f"r{i}")
                return_clauses.append(f"related{i}")
                
        # Add WHERE clause if needed
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
            
        # Add RETURN clause
        query_parts.append("RETURN " + ", ".join(return_clauses))
        
        # Add pagination
        skip = pagination.get("skip", 0)
        limit = pagination.get("limit", 100)
        query_parts.append(f"SKIP {skip} LIMIT {limit}")
        
        # Join query parts
        query = "\n".join(query_parts)
        
        # Execute query
        with self.driver.session() as session:
            result = session.run(query, parameters=parameters)
            records = result.data()
            
        # Process results based on output format
        if output_format == "json":
            # Transform records into entities and relationships
            entities = []
            relationships = []
            
            for record in records:
                # Process entity
                entity = self._record_to_dict(record.get("e"))
                if entity and entity not in entities:
                    entities.append(entity)
                
                # Process relationships
                for key, value in record.items():
                    if key.startswith("r") and value is not None:
                        rel = self._record_to_dict(value)
                        if rel and rel not in relationships:
                            relationships.append(rel)
                            
                    # Process related entities
                    if key.startswith("related") and value is not None:
                        related = self._record_to_dict(value)
                        if related and related not in entities:
                            entities.append(related)
            
            end_time = time.time()
            return {
                "entities": entities,
                "relationships": relationships,
                "query_time": end_time - start_time
            }
        elif output_format == "cypher":
            # Return Cypher query for debugging
            end_time = time.time()
            return {
                "cypher": query,
                "parameters": parameters,
                "query_time": end_time - start_time
            }
        else:
            # Default to JSON
            end_time = time.time()
            return {
                "records": records,
                "query_time": end_time - start_time
            }
            
    def _record_to_dict(self, record: Any) -> Optional[Dict[str, Any]]:
        """
        Convert Neo4j record to dictionary.
        
        Args:
            record: Neo4j record
            
        Returns:
            Dictionary representation of record or None if record is None
        """
        if record is None:
            return None
            
        if hasattr(record, "items"):
            # Handle node or relationship with properties
            return dict(record.items())
        else:
            # Try to convert to dictionary
            try:
                return dict(record)
            except (TypeError, ValueError):
                return {"value": record}
