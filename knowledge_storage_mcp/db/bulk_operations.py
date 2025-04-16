"""
Bulk operations for Knowledge Storage MCP.

This module provides functions for bulk import and export of entities and relationships,
optimized for performance with large datasets.
"""

import json
import time
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

from loguru import logger
from neo4j import Transaction

from knowledge_storage_mcp.db.neo4j_client import Neo4jClient


class BulkOperations:
    """
    Bulk operations for Knowledge Storage MCP.
    
    This class provides methods for bulk import and export of entities and relationships,
    optimized for performance with large datasets.
    """
    
    def __init__(self, neo4j_client: Neo4jClient):
        """
        Initialize bulk operations.
        
        Args:
            neo4j_client: Neo4j client instance
        """
        self.neo4j_client = neo4j_client
        
    def bulk_import_entities(
        self, 
        entities: List[Dict[str, Any]], 
        batch_size: int = 100
    ) -> Tuple[int, List[str]]:
        """
        Import entities in bulk.
        
        Args:
            entities: List of entity dictionaries with 'entity_type' and 'properties' keys
            batch_size: Number of entities to import in a single transaction
            
        Returns:
            Tuple of (number of successful imports, list of failed entity IDs)
        """
        successful_imports = 0
        failed_entities = []
        
        # Process entities in batches
        for i in range(0, len(entities), batch_size):
            batch = entities[i:i + batch_size]
            
            try:
                # Process batch in a single transaction
                with self.neo4j_client.driver.session() as session:
                    result = session.execute_write(self._import_entities_batch, batch)
                    successful_imports += result
            except Exception as e:
                logger.error(f"Error importing entity batch: {str(e)}")
                # Add all entities in the batch to failed list (simplistic approach)
                for entity in batch:
                    if entity.get("properties", {}).get("id"):
                        failed_entities.append(entity["properties"]["id"])
        
        return successful_imports, failed_entities
    
    def _import_entities_batch(self, tx: Transaction, entities: List[Dict[str, Any]]) -> int:
        """
        Import a batch of entities in a single transaction.
        
        Args:
            tx: Neo4j transaction
            entities: List of entity dictionaries
            
        Returns:
            Number of successful imports
        """
        successful_count = 0
        timestamp = self.neo4j_client.get_timestamp()
        
        for entity in entities:
            entity_type = entity.get("entity_type")
            properties = entity.get("properties", {})
            provenance = entity.get("provenance", {})
            
            if not entity_type:
                logger.warning("Skipping entity without entity_type")
                continue
                
            # Ensure entity has required properties
            if "id" not in properties:
                properties["id"] = self.neo4j_client._generate_id()
                
            if "created_at" not in properties:
                properties["created_at"] = timestamp
                
            if "updated_at" not in properties:
                properties["updated_at"] = timestamp
                
            # Add provenance as a property if provided
            if provenance:
                properties["provenance"] = provenance
                
            # Create entity in Neo4j
            query = f"""
            CREATE (e:{entity_type} $properties)
            RETURN e.id as id
            """
            
            try:
                result = tx.run(query, parameters={"properties": properties})
                if result.single():
                    successful_count += 1
            except Exception as e:
                logger.error(f"Error creating entity: {str(e)}")
                
        return successful_count
    
    def bulk_import_relationships(
        self, 
        relationships: List[Dict[str, Any]], 
        batch_size: int = 100
    ) -> Tuple[int, List[str]]:
        """
        Import relationships in bulk.
        
        Args:
            relationships: List of relationship dictionaries with 'from_entity_id', 
                           'relationship_type', 'to_entity_id', and 'properties' keys
            batch_size: Number of relationships to import in a single transaction
            
        Returns:
            Tuple of (number of successful imports, list of failed relationship IDs)
        """
        successful_imports = 0
        failed_relationships = []
        
        # Process relationships in batches
        for i in range(0, len(relationships), batch_size):
            batch = relationships[i:i + batch_size]
            
            try:
                # Process batch in a single transaction
                with self.neo4j_client.driver.session() as session:
                    result = session.execute_write(self._import_relationships_batch, batch)
                    successful_imports += result
            except Exception as e:
                logger.error(f"Error importing relationship batch: {str(e)}")
                # Add all relationships in the batch to failed list (simplistic approach)
                for relationship in batch:
                    if relationship.get("properties", {}).get("id"):
                        failed_relationships.append(relationship["properties"]["id"])
        
        return successful_imports, failed_relationships
    
    def _import_relationships_batch(self, tx: Transaction, relationships: List[Dict[str, Any]]) -> int:
        """
        Import a batch of relationships in a single transaction.
        
        Args:
            tx: Neo4j transaction
            relationships: List of relationship dictionaries
            
        Returns:
            Number of successful imports
        """
        successful_count = 0
        timestamp = self.neo4j_client.get_timestamp()
        
        for relationship in relationships:
            from_entity_id = relationship.get("from_entity_id")
            relationship_type = relationship.get("relationship_type")
            to_entity_id = relationship.get("to_entity_id")
            properties = relationship.get("properties", {})
            
            if not from_entity_id or not relationship_type or not to_entity_id:
                logger.warning("Skipping relationship with missing required fields")
                continue
                
            # Ensure relationship has required properties
            if "id" not in properties:
                properties["id"] = self.neo4j_client._generate_id()
                
            if "created_at" not in properties:
                properties["created_at"] = timestamp
                
            # Create relationship in Neo4j
            query = f"""
            MATCH (from {{id: $from_entity_id}})
            MATCH (to {{id: $to_entity_id}})
            CREATE (from)-[r:{relationship_type} $properties]->(to)
            RETURN r.id as id
            """
            
            try:
                result = tx.run(
                    query, 
                    parameters={
                        "from_entity_id": from_entity_id,
                        "to_entity_id": to_entity_id,
                        "properties": properties
                    }
                )
                if result.single():
                    successful_count += 1
            except Exception as e:
                logger.error(f"Error creating relationship: {str(e)}")
                
        return successful_count
    
    def export_subgraph(
        self,
        query_params: Dict[str, Any],
        output_file: Optional[str] = None,
        format: str = "json"
    ) -> Optional[Dict[str, Any]]:
        """
        Export a subgraph based on query parameters.
        
        Args:
            query_params: Query parameters for filtering the subgraph
            output_file: Path to output file (if None, return the data)
            format: Output format ('json' or 'cypher')
            
        Returns:
            Exported data as dictionary if output_file is None, otherwise None
        """
        start_time = time.time()
        
        # Query the knowledge graph
        result = self.neo4j_client.query_knowledge_graph(
            query_params=query_params,
            output_format="json"
        )
        
        # Add metadata
        export_data = {
            "entities": result.get("entities", []),
            "relationships": result.get("relationships", []),
            "metadata": {
                "export_time": self.neo4j_client.get_timestamp(),
                "query_params": query_params,
                "entity_count": len(result.get("entities", [])),
                "relationship_count": len(result.get("relationships", [])),
                "export_duration": time.time() - start_time
            }
        }
        
        # Handle different formats
        if format == "json":
            if output_file:
                with open(output_file, 'w') as f:
                    json.dump(export_data, f, indent=2)
                return None
            else:
                return export_data
        elif format == "cypher":
            # Generate Cypher statements for export
            statements = []
            
            # Add entity creation statements
            for entity in export_data["entities"]:
                entity_type = entity.get("type", "Entity")
                # Remove internal properties for clean export
                export_properties = {k: v for k, v in entity.items() if k not in ["type"]}
                statements.append(f"CREATE (:{entity_type} {json.dumps(export_properties)})")
            
            # Add relationship creation statements
            for relationship in export_data["relationships"]:
                from_id = relationship.get("from_entity_id")
                to_id = relationship.get("to_entity_id")
                rel_type = relationship.get("relationship_type", "RELATED_TO")
                # Remove internal properties for clean export
                export_properties = {k: v for k, v in relationship.items() 
                                    if k not in ["from_entity_id", "to_entity_id", "relationship_type"]}
                
                statements.append(
                    f"MATCH (from {{id: '{from_id}'}}), (to {{id: '{to_id}'}}) " +
                    f"CREATE (from)-[:{rel_type} {json.dumps(export_properties)}]->(to)"
                )
            
            # Write or return statements
            if output_file:
                with open(output_file, 'w') as f:
                    for statement in statements:
                        f.write(statement + ";\n")
                return None
            else:
                return {
                    "statements": statements,
                    "metadata": export_data["metadata"]
                }
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def import_from_file(self, input_file: str) -> Tuple[int, int, List[str], List[str]]:
        """
        Import data from a file.
        
        Args:
            input_file: Path to input file (JSON format)
            
        Returns:
            Tuple of (number of successful entity imports, number of successful relationship imports,
                     list of failed entity IDs, list of failed relationship IDs)
        """
        # Check file exists
        file_path = Path(input_file)
        if not file_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
            
        # Load data from file
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        # Import entities
        entities = data.get("entities", [])
        entity_data = [
            {
                "entity_type": entity.get("type", "Entity"),
                "properties": {k: v for k, v in entity.items() if k != "type"},
                "provenance": entity.get("provenance", {})
            }
            for entity in entities
        ]
        
        entity_result = self.bulk_import_entities(entity_data)
        
        # Import relationships
        relationships = data.get("relationships", [])
        relationship_data = [
            {
                "from_entity_id": rel.get("from_entity_id"),
                "relationship_type": rel.get("relationship_type", "RELATED_TO"),
                "to_entity_id": rel.get("to_entity_id"),
                "properties": {k: v for k, v in rel.items() 
                              if k not in ["from_entity_id", "to_entity_id", "relationship_type"]}
            }
            for rel in relationships
        ]
        
        relationship_result = self.bulk_import_relationships(relationship_data)
        
        return entity_result[0], relationship_result[0], entity_result[1], relationship_result[1]
