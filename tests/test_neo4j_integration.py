"""
Integration tests for Neo4j client with a real Neo4j instance.

These tests require a running Neo4j instance. They can be run with:
pytest -xvs tests/test_neo4j_integration.py

The tests use Docker to start a Neo4j instance if DOCKER_AVAILABLE is True.
Otherwise, they expect a Neo4j instance running at the default URI.
"""

import os
import time
import uuid
import unittest
import docker
from datetime import datetime
from unittest import skipIf

from knowledge_storage_mcp.db.neo4j_client import Neo4jClient

# Flag to check if Docker is available for integration tests
DOCKER_AVAILABLE = True
try:
    docker_client = docker.from_env()
    DOCKER_AVAILABLE = True
except:
    DOCKER_AVAILABLE = False

# Neo4j connection details
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")


@skipIf(not DOCKER_AVAILABLE, "Docker not available for Neo4j container")
class TestNeo4jIntegration(unittest.TestCase):
    """Integration tests for Neo4j client with a real Neo4j instance."""

    @classmethod
    def setUpClass(cls):
        """Set up the test class by starting a Neo4j container."""
        # Start Neo4j container
        cls.neo4j_container = docker_client.containers.run(
            "neo4j:5.12.0",
            ports={"7474/tcp": 7474, "7687/tcp": 7687},
            environment={
                "NEO4J_AUTH": f"{NEO4J_USER}/{NEO4J_PASSWORD}",
                "NEO4J_apoc_export_file_enabled": "true",
                "NEO4J_apoc_import_file_enabled": "true",
                "NEO4J_apoc_import_file_use__neo4j__config": "true",
            },
            detach=True,
            remove=True,
        )

        # Wait for Neo4j to start
        time.sleep(10)  # Simple wait strategy

        # Create Neo4j client
        cls.client = Neo4jClient(
            uri=NEO4J_URI,
            user=NEO4J_USER,
            password=NEO4J_PASSWORD,
        )

        # Check connection
        retries = 5
        while retries > 0:
            try:
                if cls.client.check_connection():
                    break
            except:
                pass
            time.sleep(5)
            retries -= 1

        if retries == 0:
            raise Exception("Could not connect to Neo4j")

    @classmethod
    def tearDownClass(cls):
        """Tear down the test class by stopping the Neo4j container."""
        # Close Neo4j client
        cls.client.close()

        # Stop Neo4j container
        cls.neo4j_container.stop()

    def test_entity_crud_operations(self):
        """Test CRUD operations for entities."""
        # Create a concept entity
        concept_properties = {
            "name": "Orthogonal Collocation",
            "description": "A numerical method for solving differential equations",
            "domain": "Numerical Analysis",
            "knowledge_tier": "L2",
        }

        # Create entity
        entity_id = self.client.create_entity(
            entity_type="Concept",
            properties=concept_properties,
            provenance={"source": "test"},
        )

        # Verify entity exists
        self.assertTrue(self.client.entity_exists(entity_id))

        # Query entity
        query_params = {
            "entity_types": ["Concept"],
            "properties": {"name": "Orthogonal Collocation"},
        }
        result = self.client.query_knowledge_graph(query_params)

        # Check entity in results
        self.assertEqual(len(result["entities"]), 1)
        self.assertEqual(result["entities"][0]["name"], "Orthogonal Collocation")
        self.assertEqual(result["entities"][0]["domain"], "Numerical Analysis")

    def test_relationship_operations(self):
        """Test operations for relationships."""
        # Create symbol entity
        symbol_properties = {
            "name": "alpha",
            "latex": "\\alpha",
            "context": "Heat Transfer Coefficient",
            "meaning": "Heat transfer coefficient in W/(m²·K)",
            "dimensions": "W/(m²·K)",
            "knowledge_tier": "L1",
        }
        symbol_id = self.client.create_entity(
            entity_type="Symbol",
            properties=symbol_properties,
            provenance={"source": "test"},
        )

        # Create concept entity
        concept_properties = {
            "name": "Heat Transfer Coefficient",
            "description": "Proportionality constant between heat flux and temperature difference",
            "domain": "Thermal Engineering",
            "knowledge_tier": "L1",
        }
        concept_id = self.client.create_entity(
            entity_type="Concept",
            properties=concept_properties,
            provenance={"source": "test"},
        )

        # Create relationship
        relationship_properties = {
            "context": "Heat Transfer Equations",
            "confidence": 0.95,
        }
        relationship_id = self.client.create_relationship(
            from_entity_id=symbol_id,
            relationship_type="REPRESENTS",
            to_entity_id=concept_id,
            properties=relationship_properties,
        )

        # Query relationship
        query_params = {
            "entity_types": ["Symbol"],
            "relationships": [
                {
                    "type": "REPRESENTS",
                    "direction": "outgoing",
                    "target_type": "Concept",
                }
            ],
        }
        result = self.client.query_knowledge_graph(query_params)

        # Check results
        self.assertTrue(len(result["entities"]) >= 2)  # At least symbol and concept
        self.assertTrue(len(result["relationships"]) >= 1)  # At least one relationship

        # Find the relationship
        found_relationship = False
        for rel in result["relationships"]:
            if rel.get("id") == relationship_id:
                found_relationship = True
                self.assertEqual(rel.get("context"), "Heat Transfer Equations")
                self.assertEqual(rel.get("confidence"), 0.95)
        self.assertTrue(found_relationship)

    def test_complex_query(self):
        """Test complex query with multiple conditions."""
        # Create several entities and relationships for testing
        # Document entity
        document_properties = {
            "name": "Heat Transfer in Engineering Applications",
            "title": "Heat Transfer in Engineering Applications",
            "authors": ["Smith, J.R.", "Johnson, A.B."],
            "year": 2022,
            "knowledge_tier": "L2",
        }
        document_id = self.client.create_entity(
            entity_type="Document",
            properties=document_properties,
            provenance={"source": "test"},
        )

        # Symbol entity
        symbol_properties = {
            "name": "beta",
            "latex": "\\beta",
            "context": "Thermal Expansion Coefficient",
            "meaning": "Thermal expansion coefficient in 1/K",
            "dimensions": "1/K",
            "knowledge_tier": "L1",
        }
        symbol_id = self.client.create_entity(
            entity_type="Symbol",
            properties=symbol_properties,
            provenance={"source": "test"},
        )

        # Concept entity
        concept_properties = {
            "name": "Thermal Expansion Coefficient",
            "description": "Measure of material expansion due to temperature change",
            "domain": "Thermal Engineering",
            "knowledge_tier": "L1",
        }
        concept_id = self.client.create_entity(
            entity_type="Concept",
            properties=concept_properties,
            provenance={"source": "test"},
        )

        # Create relationships
        # Document CONTAINS Symbol
        self.client.create_relationship(
            from_entity_id=document_id,
            relationship_type="CONTAINS",
            to_entity_id=symbol_id,
            properties={"section": "2.3", "page": 7},
        )

        # Symbol REPRESENTS Concept
        self.client.create_relationship(
            from_entity_id=symbol_id,
            relationship_type="REPRESENTS",
            to_entity_id=concept_id,
            properties={"context": "Thermal Engineering", "confidence": 0.98},
        )

        # Complex query: Find symbols in the document that represent concepts
        query_params = {
            "entity_types": ["Document"],
            "properties": {"title": "Heat Transfer in Engineering Applications"},
            "relationships": [
                {
                    "type": "CONTAINS",
                    "direction": "outgoing",
                    "target_type": "Symbol",
                }
            ],
        }
        result = self.client.query_knowledge_graph(query_params)

        # Check results
        self.assertTrue(len(result["entities"]) >= 2)  # At least document and symbol
        
        # Find the document and symbol
        document_found = False
        symbol_found = False
        
        for entity in result["entities"]:
            if entity.get("id") == document_id:
                document_found = True
            if entity.get("id") == symbol_id:
                symbol_found = True
                
        self.assertTrue(document_found)
        self.assertTrue(symbol_found)

    def test_large_query_performance(self):
        """Test query performance with a larger dataset."""
        # Create multiple entities for performance testing
        entity_count = 20  # Create 20 entities
        entity_ids = []
        
        for i in range(entity_count):
            # Create symbol entities
            symbol_properties = {
                "name": f"symbol_{i}",
                "latex": f"\\symbol_{i}",
                "context": "Test Context",
                "knowledge_tier": "L1",
            }
            entity_id = self.client.create_entity(
                entity_type="Symbol",
                properties=symbol_properties,
                provenance={"source": "test"},
            )
            entity_ids.append(entity_id)

        # Time query performance
        start_time = time.time()
        
        query_params = {
            "entity_types": ["Symbol"],
            "filters": {"name": {"operator": "CONTAINS", "value": "symbol_"}},
        }
        result = self.client.query_knowledge_graph(query_params)
        
        end_time = time.time()
        query_time = end_time - start_time
        
        # Check results
        self.assertTrue(len(result["entities"]) >= entity_count)
        self.assertLess(query_time, 1.0)  # Query should be fast (< 1 second)


if __name__ == "__main__":
    unittest.main()
