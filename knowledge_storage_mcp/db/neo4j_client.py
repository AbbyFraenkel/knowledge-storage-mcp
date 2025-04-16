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
        
        # Create indexes if they don't exist
        self._ensure_indexes()
        
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
            
    def _ensure_indexes(self):
        """Ensure indexes exist for efficient queries."""
        try:
            with self.driver.session() as session:
                # Create index on entity ID
                session.run("CREATE INDEX entity_id IF NOT EXISTS FOR (e) ON (e.id)")
                # Create index on entity type
                session.run("CREATE INDEX entity_type IF NOT EXISTS FOR (e:Entity) ON (e:Entity)")
                # Create full-text index on names for fuzzy search
                session.run("""
                    CALL db.index.fulltext.createNodeIndex(
                        'entity_name_search',
                        ['Symbol', 'Concept', 'Algorithm', 'Implementation', 'Document', 'Domain'],
                        ['name', 'title', 'description']
                    )
                """)
        except Exception as e:
            logger.warning(f"Could not create indexes: {str(e)}")

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
    
    def _generate_id(self) -> str:
        """
        Generate a unique ID.
        
        Returns:
            Unique ID string
        """
        return str(uuid.uuid4())
            
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
        # Generate a unique ID if not provided
        entity_id = properties.get("id")
        if not entity_id:
            entity_id = self._generate_id()
            properties["id"] = entity_id
        
        # Add metadata to properties
        timestamp = self.get_timestamp()
        entity_properties = {
            "id": entity_id,
            "type": entity_type,  # Store entity type as a property for easier serialization
            "created_at": properties.get("created_at", timestamp),
            "updated_at": properties.get("updated_at", timestamp),
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
    
    def get_entity(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an entity by ID.
        
        Args:
            entity_id: Entity ID to retrieve
            
        Returns:
            Entity properties or None if not found
        """
        query = """
        MATCH (e {id: $entity_id})
        RETURN e
        """
        
        with self.driver.session() as session:
            result = session.run(query, parameters={"entity_id": entity_id})
            record = result.single()
            if record:
                return dict(record["e"].items())
            return None
    
    def update_entity(self, entity_id: str, properties: Dict[str, Any]) -> bool:
        """
        Update an entity's properties.
        
        Args:
            entity_id: Entity ID to update
            properties: New properties to set
            
        Returns:
            True if update was successful, False otherwise
        """
        # Update timestamp
        update_properties = {
            **properties,
            "updated_at": self.get_timestamp()
        }
        
        query = """
        MATCH (e {id: $entity_id})
        SET e += $properties
        RETURN e.id as id
        """
        
        with self.driver.session() as session:
            result = session.run(
                query, 
                parameters={
                    "entity_id": entity_id,
                    "properties": update_properties
                }
            )
            return result.single() is not None
    
    def delete_entity(self, entity_id: str) -> bool:
        """
        Delete an entity and its relationships.
        
        Args:
            entity_id: Entity ID to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        query = """
        MATCH (e {id: $entity_id})
        DETACH DELETE e
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
        # Generate a unique ID if not provided
        relationship_id = properties.get("id")
        if not relationship_id:
            relationship_id = self._generate_id()
        
        # Add metadata to properties
        relationship_properties = {
            "id": relationship_id,
            "created_at": properties.get("created_at", self.get_timestamp()),
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
    
    def relationship_exists(self, relationship_id: str) -> bool:
        """
        Check if a relationship exists in the knowledge graph.
        
        Args:
            relationship_id: Relationship ID to check
            
        Returns:
            True if relationship exists, False otherwise
        """
        query = """
        MATCH ()-[r {id: $relationship_id}]->()
        RETURN count(r) as count
        """
        
        with self.driver.session() as session:
            result = session.run(query, parameters={"relationship_id": relationship_id})
            return result.single()["count"] > 0
    
    def get_relationship(self, relationship_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a relationship by ID.
        
        Args:
            relationship_id: Relationship ID to retrieve
            
        Returns:
            Relationship properties or None if not found
        """
        query = """
        MATCH (from)-[r {id: $relationship_id}]->(to)
        RETURN r, from.id as from_entity_id, to.id as to_entity_id, type(r) as relationship_type
        """
        
        with self.driver.session() as session:
            result = session.run(query, parameters={"relationship_id": relationship_id})
            record = result.single()
            if record:
                rel_props = dict(record["r"].items())
                rel_props["from_entity_id"] = record["from_entity_id"]
                rel_props["to_entity_id"] = record["to_entity_id"]
                rel_props["relationship_type"] = record["relationship_type"]
                return rel_props
            return None
    
    def update_relationship(self, relationship_id: str, properties: Dict[str, Any]) -> bool:
        """
        Update a relationship's properties.
        
        Args:
            relationship_id: Relationship ID to update
            properties: New properties to set
            
        Returns:
            True if update was successful, False otherwise
        """
        query = """
        MATCH ()-[r {id: $relationship_id}]->()
        SET r += $properties
        RETURN r.id as id
        """
        
        with self.driver.session() as session:
            result = session.run(
                query, 
                parameters={
                    "relationship_id": relationship_id,
                    "properties": properties
                }
            )
            return result.single() is not None
    
    def delete_relationship(self, relationship_id: str) -> bool:
        """
        Delete a relationship.
        
        Args:
            relationship_id: Relationship ID to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        query = """
        MATCH ()-[r {id: $relationship_id}]->()
        DELETE r
        RETURN count(r) as count
        """
        
        with self.driver.session() as session:
            result = session.run(query, parameters={"relationship_id": relationship_id})
            return result.single()["count"] > 0
            
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
        
        # Check if this is a fulltext search query
        if "text_search" in query_params:
            return self._fulltext_search(
                query_params.get("text_search", ""),
                query_params.get("entity_types", []),
                query_params.get("limit", 100),
                output_format
            )
        
        # Extract query parameters
        entity_types = query_params.get("entity_types", [])
        properties = query_params.get("properties", {})
        relationships = query_params.get("relationships", [])
        filters = query_params.get("filters", {})
        pagination = query_params.get("pagination", {"skip": 0, "limit": 100})
        
        # Build Cypher query
        query_parts = []
        where_clauses = []
        return_clauses = ["e"]
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
                            rel["from_entity_id"] = entity["id"]  # Add source entity ID
                            if f"related{key[1:]}" in record and record[f"related{key[1:]}"] is not None:
                                rel["to_entity_id"] = record[f"related{key[1:]}"]["id"]  # Add target entity ID
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
            # Default to raw records
            end_time = time.time()
            return {
                "records": records,
                "query_time": end_time - start_time
            }
    
    def _fulltext_search(
        self, 
        search_text: str, 
        entity_types: List[str] = None,
        limit: int = 100,
        output_format: str = "json"
    ) -> Dict[str, Any]:
        """
        Perform a fulltext search on the knowledge graph.
        
        Args:
            search_text: Text to search for
            entity_types: Filter by entity types
            limit: Maximum number of results
            output_format: Output format
            
        Returns:
            Search results
        """
        start_time = time.time()
        
        # Build entity type filter
        entity_filter = ""
        if entity_types:
            entity_list = ", ".join([f"'{t}'" for t in entity_types])
            entity_filter = f"AND labels(node) IN [{entity_list}]"
        
        # Build search query
        query = f"""
        CALL db.index.fulltext.queryNodes('entity_name_search', $search_text)
        YIELD node, score
        WHERE score > 0 {entity_filter}
        RETURN node as e, score
        ORDER BY score DESC
        LIMIT $limit
        """
        
        parameters = {
            "search_text": search_text,
            "limit": limit
        }
        
        # Execute query
        with self.driver.session() as session:
            result = session.run(query, parameters=parameters)
            records = result.data()
        
        # Process results
        if output_format == "json":
            entities = []
            for record in records:
                entity = self._record_to_dict(record.get("e"))
                if entity:
                    entity["search_score"] = record.get("score", 0)
                    entities.append(entity)
            
            end_time = time.time()
            return {
                "entities": entities,
                "relationships": [],
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
            # Default to raw records
            end_time = time.time()
            return {
                "records": records,
                "query_time": end_time - start_time
            }
    
    def similar_entities(
        self,
        entity_id: str,
        min_similarity: float = 0.5,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Find similar entities based on shared properties and relationships.
        
        Args:
            entity_id: Entity ID to find similar entities for
            min_similarity: Minimum similarity score (0.0-1.0)
            limit: Maximum number of results
            
        Returns:
            List of similar entities with similarity scores
        """
        query = """
        MATCH (e {id: $entity_id})
        MATCH (other)
        WHERE other.id <> $entity_id
        AND labels(other) = labels(e)
        
        // Calculate property similarity
        WITH e, other, 
            apoc.text.jaroWinklerDistance(coalesce(e.name,''), coalesce(other.name,'')) as name_sim,
            apoc.text.jaroWinklerDistance(coalesce(e.description,''), coalesce(other.description,'')) as desc_sim,
            (CASE WHEN e.knowledge_tier = other.knowledge_tier THEN 1.0 ELSE 0.0 END) as tier_sim
        
        // Calculate relationship similarity
        OPTIONAL MATCH (e)-[r1]-(common)
        OPTIONAL MATCH (other)-[r2]-(common)
        WHERE type(r1) = type(r2)
        
        WITH e, other, name_sim, desc_sim, tier_sim, count(common) as common_relations
        
        // Get total relations for both entities
        OPTIONAL MATCH (e)-[r_e]-()
        WITH e, other, name_sim, desc_sim, tier_sim, common_relations, count(r_e) as total_e_relations
        
        OPTIONAL MATCH (other)-[r_other]-()
        WITH e, other, name_sim, desc_sim, tier_sim, common_relations, total_e_relations, 
             count(r_other) as total_other_relations
        
        // Calculate relationship similarity
        WITH e, other, name_sim, desc_sim, tier_sim, common_relations,
             CASE 
                WHEN total_e_relations + total_other_relations - common_relations > 0 
                THEN common_relations * 1.0 / (total_e_relations + total_other_relations - common_relations)
                ELSE 0
             END as relation_sim
        
        // Calculate overall similarity
        WITH e, other, (name_sim * 0.4 + desc_sim * 0.3 + tier_sim * 0.1 + relation_sim * 0.2) as similarity
        WHERE similarity >= $min_similarity
        
        RETURN other as entity, similarity
        ORDER BY similarity DESC
        LIMIT $limit
        """
        
        with self.driver.session() as session:
            result = session.run(
                query, 
                parameters={
                    "entity_id": entity_id,
                    "min_similarity": min_similarity,
                    "limit": limit
                }
            )
            
            similar_entities = []
            for record in result:
                entity = self._record_to_dict(record["entity"])
                similarity = record["similarity"]
                similar_entities.append({
                    "entity": entity,
                    "similarity": similarity
                })
                
            return similar_entities
            
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
    
    def get_bulk_operations(self):
        """
        Get a BulkOperations instance for this client.
        
        Returns:
            BulkOperations instance
        """
        # Import here to avoid circular imports
        from knowledge_storage_mcp.db.bulk_operations import BulkOperations
        return BulkOperations(self)
