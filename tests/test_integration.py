"""
Integration tests for Knowledge Storage MCP with Neo4j.

These tests use a real Neo4j database to validate the functionality
of the Knowledge Storage MCP components.
"""

import unittest
import os
import time
from datetime import datetime

import docker
import pytest
from neo4j import GraphDatabase

from knowledge_storage_mcp.db.neo4j_client import Neo4jClient
from knowledge_storage_mcp.schema.validator import SchemaValidator
from knowledge_storage_mcp.schema.entity_types import KnowledgeTier


@pytest.mark.integration
class TestNeo4jIntegration(unittest.TestCase):
    """Integration tests with Neo4j database."""
    
    @classmethod
    def setUpClass(cls):
        """Set up Neo4j container and client."""
        # Set up Neo4j container using Docker
        cls.docker_client = docker.from_env()
        
        # Check if the container is already running
        existing_containers = cls.docker_client.containers.list(
            filters={"name": "neo4j-test"}
        )
        
        if existing_containers:
            cls.neo4j_container = existing_containers[0]
            print("Using existing Neo4j container")
        else:
            # Start a new container
            cls.neo4j_container = cls.docker_client.containers.run(
                "neo4j:5.12.0",
                name="neo4j-test",
                environment={
                    "NEO4J_AUTH": "neo4j/password",
                    "NEO4J_apoc_export_file_enabled": "true",
                    "NEO4J_apoc_import_file_enabled": "true",
                    "NEO4J_apoc_import_file_use__neo4j__config": "true"
                },
                ports={
                    "7474/tcp": 7475,  # HTTP
                    "7687/tcp": 7688   # Bolt
                },
                detach=True,
                remove=True
            )
            print("Started new Neo4j container")
            
            # Wait for Neo4j to start
            time.sleep(20)  # Adjust as needed
            
        # Initialize Neo4j client
        cls.neo4j_client = Neo4jClient(
            uri="bolt://localhost:7688",
            user="neo4j",
            password="password"
        )
        
        # Initialize schema validator
        cls.validator = SchemaValidator(enabled=True)
        
        # Verify connection
        retry_count = 0
        max_retries = 5
        while retry_count < max_retries:
            try:
                if cls.neo4j_client.check_connection():
                    print("Connected to Neo4j successfully")
                    break
            except Exception as e:
                print(f"Connection attempt {retry_count + 1} failed: {str(e)}")
                retry_count += 1
                time.sleep(5)
                
        if retry_count == max_retries:
            raise ConnectionError("Failed to connect to Neo4j after multiple attempts")
            
    @classmethod
    def tearDownClass(cls):
        """Tear down resources."""
        # Close Neo4j client connection
        if hasattr(cls, "neo4j_client"):
            cls.neo4j_client.close()
            
        # Leave the container running for subsequent tests
        # It will be removed when the Docker client is closed due to the 'remove=True' parameter
            
    def setUp(self):
        """Set up test database state."""
        # Clear database before each test
        self._clear_database()
        
    def _clear_database(self):
        """Clear all data from the database."""
        with self.neo4j_client.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            
    def _count_nodes(self):
        """Count nodes in the database."""
        with self.neo4j_client.driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count")
            return result.single()["count"]
            
    def _count_relationships(self):
        """Count relationships in the database."""
        with self.neo4j_client.driver.session() as session:
            result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
            return result.single()["count"]
            
    def test_create_entity(self):
        """Test creating an entity in Neo4j."""
        # Test data
        entity_type = "Concept"
        entity_properties = {
            "name": "Orthogonal Collocation",
            "description": "A numerical method for solving differential equations",
            "domain": "Numerical Analysis",
            "knowledge_tier": KnowledgeTier.L2.value,
            "created_at": self.neo4j_client.get_timestamp(),
            "updated_at": self.neo4j_client.get_timestamp()
        }
        
        # Create entity
        entity_id = self.neo4j_client.create_entity(
            entity_type=entity_type,
            properties=entity_properties,
            provenance={"source": "integration test"}
        )
        
        # Verify entity exists
        self.assertTrue(self.neo4j_client.entity_exists(entity_id))
        
        # Verify node count
        self.assertEqual(self._count_nodes(), 1)
        
        # Verify entity properties
        with self.neo4j_client.driver.session() as session:
            result = session.run(
                "MATCH (e:Concept {id: $id}) RETURN e",
                id=entity_id
            )
            node = result.single()["e"]
            
            self.assertEqual(node["name"], "Orthogonal Collocation")
            self.assertEqual(node["description"], "A numerical method for solving differential equations")
            self.assertEqual(node["knowledge_tier"], KnowledgeTier.L2.value)
            
    def test_create_symbol_concept_relationship(self):
        """Test creating a symbol-concept relationship in Neo4j."""
        # Create symbol entity
        symbol_properties = {
            "name": "alpha",
            "latex": "\\alpha",
            "context": "Heat Transfer Coefficient",
            "meaning": "Heat transfer coefficient in W/(m²·K)",
            "dimensions": "W/(m²·K)",
            "knowledge_tier": KnowledgeTier.L1.value,
            "created_at": self.neo4j_client.get_timestamp(),
            "updated_at": self.neo4j_client.get_timestamp()
        }
        
        symbol_id = self.neo4j_client.create_entity(
            entity_type="Symbol",
            properties=symbol_properties,
            provenance={"source": "integration test"}
        )
        
        # Create concept entity
        concept_properties = {
            "name": "Heat Transfer Coefficient",
            "description": "A measure of the heat transfer between a solid surface and a fluid",
            "domain": "Heat Transfer",
            "knowledge_tier": KnowledgeTier.L2.value,
            "created_at": self.neo4j_client.get_timestamp(),
            "updated_at": self.neo4j_client.get_timestamp()
        }
        
        concept_id = self.neo4j_client.create_entity(
            entity_type="Concept",
            properties=concept_properties,
            provenance={"source": "integration test"}
        )
        
        # Create relationship
        relationship_properties = {
            "context": "Heat Transfer Equations",
            "confidence": 0.95,
            "created_at": self.neo4j_client.get_timestamp()
        }
        
        relationship_id = self.neo4j_client.create_relationship(
            from_entity_id=symbol_id,
            relationship_type="REPRESENTS",
            to_entity_id=concept_id,
            properties=relationship_properties
        )
        
        # Verify relationship exists
        with self.neo4j_client.driver.session() as session:
            result = session.run(
                """
                MATCH (s:Symbol {id: $symbol_id})-[r:REPRESENTS]->(c:Concept {id: $concept_id})
                RETURN r
                """,
                symbol_id=symbol_id,
                concept_id=concept_id
            )
            
            relationship = result.single()["r"]
            
            self.assertEqual(relationship["context"], "Heat Transfer Equations")
            self.assertEqual(relationship["confidence"], 0.95)
            
        # Verify node and relationship counts
        self.assertEqual(self._count_nodes(), 2)
        self.assertEqual(self._count_relationships(), 1)
        
    def test_query_knowledge_graph(self):
        """Test querying the knowledge graph."""
        # Create multiple entities
        symbol1_id = self.neo4j_client.create_entity(
            entity_type="Symbol",
            properties={
                "name": "alpha",
                "latex": "\\alpha",
                "context": "Heat Transfer",
                "knowledge_tier": KnowledgeTier.L1.value,
                "created_at": self.neo4j_client.get_timestamp(),
                "updated_at": self.neo4j_client.get_timestamp()
            },
            provenance={"source": "integration test"}
        )
        
        symbol2_id = self.neo4j_client.create_entity(
            entity_type="Symbol",
            properties={
                "name": "beta",
                "latex": "\\beta",
                "context": "Heat Transfer",
                "knowledge_tier": KnowledgeTier.L1.value,
                "created_at": self.neo4j_client.get_timestamp(),
                "updated_at": self.neo4j_client.get_timestamp()
            },
            provenance={"source": "integration test"}
        )
        
        concept_id = self.neo4j_client.create_entity(
            entity_type="Concept",
            properties={
                "name": "Heat Transfer Coefficient",
                "domain": "Heat Transfer",
                "knowledge_tier": KnowledgeTier.L2.value,
                "created_at": self.neo4j_client.get_timestamp(),
                "updated_at": self.neo4j_client.get_timestamp()
            },
            provenance={"source": "integration test"}
        )
        
        # Create relationships
        self.neo4j_client.create_relationship(
            from_entity_id=symbol1_id,
            relationship_type="REPRESENTS",
            to_entity_id=concept_id,
            properties={
                "context": "Engineering",
                "confidence": 0.9,
                "created_at": self.neo4j_client.get_timestamp()
            }
        )
        
        self.neo4j_client.create_relationship(
            from_entity_id=symbol2_id,
            relationship_type="RELATED_TO",
            to_entity_id=symbol1_id,
            properties={
                "created_at": self.neo4j_client.get_timestamp()
            }
        )
        
        # Query by entity type
        query_result = self.neo4j_client.query_knowledge_graph(
            query_params={
                "entity_types": ["Symbol"]
            }
        )
        
        self.assertEqual(len(query_result["entities"]), 2)
        
        # Query with property filter
        query_result = self.neo4j_client.query_knowledge_graph(
            query_params={
                "entity_types": ["Symbol"],
                "properties": {
                    "name": "alpha"
                }
            }
        )
        
        self.assertEqual(len(query_result["entities"]), 1)
        self.assertEqual(query_result["entities"][0]["name"], "alpha")
        
        # Query with relationship
        query_result = self.neo4j_client.query_knowledge_graph(
            query_params={
                "entity_types": ["Symbol"],
                "relationships": [
                    {
                        "type": "REPRESENTS",
                        "target_type": "Concept",
                        "direction": "outgoing"
                    }
                ]
            }
        )
        
        symbol_entities = [e for e in query_result["entities"] if "Symbol" in str(e)]
        self.assertEqual(len(symbol_entities), 1)
        self.assertEqual(symbol_entities[0]["name"], "alpha")
        
    def test_bulk_entity_creation(self):
        """Test creating multiple entities in bulk."""
        # Create multiple entities
        entity_data = [
            {
                "entity_type": "Symbol",
                "properties": {
                    "name": f"symbol{i}",
                    "latex": f"\\symbol{i}",
                    "context": "Test Context",
                    "knowledge_tier": KnowledgeTier.L1.value,
                    "created_at": self.neo4j_client.get_timestamp(),
                    "updated_at": self.neo4j_client.get_timestamp()
                },
                "provenance": {"source": "bulk test"}
            }
            for i in range(5)
        ]
        
        # Create entities using transaction
        with self.neo4j_client.driver.session() as session:
            with session.begin_transaction() as tx:
                for entity in entity_data:
                    tx.run(
                        f"""
                        CREATE (e:{entity['entity_type']} $properties)
                        RETURN e.id as id
                        """,
                        properties={
                            "id": f"bulk-{entity['properties']['name']}",
                            **entity["properties"]
                        }
                    )
                    
        # Verify node count
        self.assertEqual(self._count_nodes(), 5)
        
        # Query all symbols
        query_result = self.neo4j_client.query_knowledge_graph(
            query_params={
                "entity_types": ["Symbol"]
            }
        )
        
        self.assertEqual(len(query_result["entities"]), 5)


if __name__ == "__main__":
    unittest.main()
