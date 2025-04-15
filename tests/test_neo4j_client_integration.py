"""
Integration tests for the Neo4j client in Knowledge Storage MCP.

These tests use testcontainers to spin up an isolated Neo4j instance
for testing and verify that all Neo4j client functionality works
correctly with an actual Neo4j database.
"""

import os
import unittest
from unittest.mock import patch
from datetime import datetime

import pytest
from testcontainers.neo4j import Neo4jContainer

from knowledge_storage_mcp.db.neo4j_client import Neo4jClient
from knowledge_storage_mcp.schema.entity_types import KnowledgeTier


class TestNeo4jClientIntegration(unittest.TestCase):
    """Integration tests for Neo4jClient with a real Neo4j instance."""

    @classmethod
    def setUpClass(cls):
        """Set up a Neo4j container for testing."""
        # Start Neo4j container
        cls.neo4j_container = Neo4jContainer("neo4j:5.12.0")
        cls.neo4j_container.start()
        
        # Get connection details
        cls.neo4j_uri = cls.neo4j_container.get_connection_url()
        cls.neo4j_user = "neo4j"
        cls.neo4j_password = "password"  # Default password in testcontainer
        
        # Create client
        cls.client = Neo4jClient(
            uri=cls.neo4j_uri,
            user=cls.neo4j_user,
            password=cls.neo4j_password
        )
        
        # Test timestamp for unique entity IDs across test runs
        cls.timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    @classmethod
    def tearDownClass(cls):
        """Tear down the Neo4j container."""
        if hasattr(cls, 'client') and cls.client:
            cls.client.close()
            
        if hasattr(cls, 'neo4j_container') and cls.neo4j_container:
            cls.neo4j_container.stop()

    def setUp(self):
        """Set up test case - create a clean database state."""
        # Clear the database before each test (optional)
        with self.client.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def test_connection(self):
        """Test that the connection to Neo4j works."""
        # Check if the connection is successful
        self.assertTrue(self.client.check_connection())

    def test_create_entity(self):
        """Test creating and retrieving an entity."""
        # Create a concept entity
        entity_properties = {
            "name": "Test Concept",
            "description": "A test concept for integration testing",
            "knowledge_tier": KnowledgeTier.L1.value
        }
        
        entity_id = self.client.create_entity(
            entity_type="Concept",
            properties=entity_properties,
            provenance={"source": "integration_test"}
        )
        
        # Verify entity exists
        self.assertTrue(self.client.entity_exists(entity_id))
        
        # Retrieve and verify entity
        with self.client.driver.session() as session:
            result = session.run(
                "MATCH (e:Concept {id: $id}) RETURN e",
                parameters={"id": entity_id}
            )
            record = result.single()
            self.assertIsNotNone(record)
            
            entity = record["e"]
            self.assertEqual(entity["name"], "Test Concept")
            self.assertEqual(entity["description"], "A test concept for integration testing")
            self.assertEqual(entity["knowledge_tier"], KnowledgeTier.L1.value)
            self.assertTrue("created_at" in entity)
            self.assertTrue("updated_at" in entity)
            self.assertEqual(entity["provenance"]["source"], "integration_test")

    def test_create_relationship(self):
        """Test creating and retrieving a relationship between entities."""
        # Create two entities
        concept_properties = {
            "name": "Test Concept",
            "description": "A test concept for integration testing",
            "knowledge_tier": KnowledgeTier.L1.value
        }
        
        symbol_properties = {
            "name": "Test Symbol",
            "latex": "\\alpha",
            "context": "Test Context",
            "knowledge_tier": KnowledgeTier.L1.value
        }
        
        concept_id = self.client.create_entity(
            entity_type="Concept",
            properties=concept_properties,
            provenance={"source": "integration_test"}
        )
        
        symbol_id = self.client.create_entity(
            entity_type="Symbol",
            properties=symbol_properties,
            provenance={"source": "integration_test"}
        )
        
        # Create relationship
        relationship_properties = {
            "context": "Integration Testing",
            "confidence": 0.95
        }
        
        relationship_id = self.client.create_relationship(
            from_entity_id=symbol_id,
            relationship_type="REPRESENTS",
            to_entity_id=concept_id,
            properties=relationship_properties
        )
        
        # Verify relationship
        with self.client.driver.session() as session:
            result = session.run(
                """
                MATCH (s {id: $symbol_id})-[r:REPRESENTS]->(c {id: $concept_id})
                RETURN r
                """,
                parameters={
                    "symbol_id": symbol_id,
                    "concept_id": concept_id
                }
            )
            record = result.single()
            self.assertIsNotNone(record)
            
            relationship = record["r"]
            self.assertEqual(relationship["id"], relationship_id)
            self.assertEqual(relationship["context"], "Integration Testing")
            self.assertEqual(relationship["confidence"], 0.95)
            self.assertTrue("created_at" in relationship)

    def test_query_knowledge_graph(self):
        """Test querying the knowledge graph."""
        # Create multiple entities and relationships
        for i in range(3):
            # Create concepts
            concept_properties = {
                "name": f"Concept {i}",
                "description": f"Test concept {i}",
                "knowledge_tier": KnowledgeTier.L1.value,
                "test_property": f"value_{i}"
            }
            
            concept_id = self.client.create_entity(
                entity_type="Concept",
                properties=concept_properties,
                provenance={"source": "integration_test"}
            )
            
            # Create symbols
            symbol_properties = {
                "name": f"Symbol {i}",
                "latex": f"\\alpha_{i}",
                "context": "Test Context",
                "knowledge_tier": KnowledgeTier.L1.value
            }
            
            symbol_id = self.client.create_entity(
                entity_type="Symbol",
                properties=symbol_properties,
                provenance={"source": "integration_test"}
            )
            
            # Create relationships
            relationship_properties = {
                "context": f"Relationship {i}",
                "confidence": 0.8 + (i * 0.05)
            }
            
            self.client.create_relationship(
                from_entity_id=symbol_id,
                relationship_type="REPRESENTS",
                to_entity_id=concept_id,
                properties=relationship_properties
            )
        
        # Test basic query - filter by entity type
        query_params = {
            "entity_types": ["Concept"],
            "pagination": {"skip": 0, "limit": 10}
        }
        
        results = self.client.query_knowledge_graph(query_params)
        self.assertEqual(len(results["entities"]), 3)
        self.assertEqual(len(results["relationships"]), 0)  # No relationships because we only matched entities
        
        # Test query with properties filter
        query_params = {
            "entity_types": ["Concept"],
            "properties": {"test_property": "value_1"},
            "pagination": {"skip": 0, "limit": 10}
        }
        
        results = self.client.query_knowledge_graph(query_params)
        self.assertEqual(len(results["entities"]), 1)
        self.assertEqual(results["entities"][0]["name"], "Concept 1")
        
        # Test query with relationships
        query_params = {
            "entity_types": ["Symbol"],
            "relationships": [
                {
                    "type": "REPRESENTS",
                    "direction": "outgoing",
                    "target_type": "Concept"
                }
            ],
            "pagination": {"skip": 0, "limit": 10}
        }
        
        results = self.client.query_knowledge_graph(query_params)
        self.assertGreaterEqual(len(results["entities"]), 3)  # Should include symbols and concepts
        self.assertGreaterEqual(len(results["relationships"]), 3)  # Should include the REPRESENTS relationships
        
        # Test pagination
        query_params = {
            "entity_types": ["Concept"],
            "pagination": {"skip": 1, "limit": 1}
        }
        
        results = self.client.query_knowledge_graph(query_params)
        self.assertEqual(len(results["entities"]), 1)  # Should only return one concept

    def test_error_handling(self):
        """Test error handling in the Neo4j client."""
        # Test entity not found
        self.assertFalse(self.client.entity_exists("non_existent_id"))
        
        # Test creating relationship with non-existent entities
        with self.assertRaises(ValueError):
            self.client.create_relationship(
                from_entity_id="non_existent_id",
                relationship_type="REPRESENTS",
                to_entity_id="another_non_existent_id",
                properties={}
            )
            
        # Test creating an entity and then attempting to create a relationship
        # from that entity to a non-existent entity
        entity_properties = {
            "name": "Error Test Entity",
            "description": "Entity for testing error handling",
            "knowledge_tier": KnowledgeTier.L1.value
        }
        
        entity_id = self.client.create_entity(
            entity_type="Concept",
            properties=entity_properties,
            provenance={"source": "integration_test"}
        )
        
        with self.assertRaises(ValueError):
            self.client.create_relationship(
                from_entity_id=entity_id,
                relationship_type="REPRESENTS",
                to_entity_id="non_existent_id",
                properties={}
            )

    def test_large_query_performance(self):
        """Test performance with larger data sets and complex queries."""
        # Skip this test in CI environments where performance might vary
        if os.environ.get("CI") == "true":
            self.skipTest("Skipping performance test in CI environment")
            
        # Create a larger dataset for testing
        num_entities = 50  # Adjust based on test environment capabilities
        
        # Create entities
        concept_ids = []
        symbol_ids = []
        
        # Batch entity creation for better performance
        with self.client.driver.session() as session:
            # Create concepts
            concepts_query = """
            UNWIND $concepts AS concept
            CREATE (c:Concept)
            SET c = concept
            RETURN c.id AS id
            """
            
            concepts = []
            for i in range(num_entities):
                concept_id = f"concept_{self.timestamp}_{i}"
                concept_ids.append(concept_id)
                concepts.append({
                    "id": concept_id,
                    "name": f"Concept {i}",
                    "description": f"Test concept {i}",
                    "knowledge_tier": KnowledgeTier.L1.value,
                    "test_property": f"value_{i % 5}",  # Create some duplicate property values
                    "created_at": self.client.get_timestamp(),
                    "updated_at": self.client.get_timestamp()
                })
            
            result = session.run(concepts_query, parameters={"concepts": concepts})
            
            # Create symbols
            symbols_query = """
            UNWIND $symbols AS symbol
            CREATE (s:Symbol)
            SET s = symbol
            RETURN s.id AS id
            """
            
            symbols = []
            for i in range(num_entities):
                symbol_id = f"symbol_{self.timestamp}_{i}"
                symbol_ids.append(symbol_id)
                symbols.append({
                    "id": symbol_id,
                    "name": f"Symbol {i}",
                    "latex": f"\\alpha_{i}",
                    "context": f"Test Context {i % 10}",  # Group contexts
                    "knowledge_tier": KnowledgeTier.L1.value,
                    "created_at": self.client.get_timestamp(),
                    "updated_at": self.client.get_timestamp()
                })
            
            result = session.run(symbols_query, parameters={"symbols": symbols})
            
            # Create relationships
            relationships_query = """
            UNWIND $relationships AS rel
            MATCH (from {id: rel.from_id})
            MATCH (to {id: rel.to_id})
            CREATE (from)-[r:REPRESENTS]->(to)
            SET r = rel.properties
            RETURN r.id AS id
            """
            
            relationships = []
            for i in range(num_entities):
                relationships.append({
                    "from_id": symbol_ids[i],
                    "to_id": concept_ids[i],
                    "properties": {
                        "id": f"rel_{self.timestamp}_{i}",
                        "context": f"Relationship {i % 10}",  # Group contexts
                        "confidence": 0.8 + ((i % 5) * 0.05),  # Group confidence levels
                        "created_at": self.client.get_timestamp()
                    }
                })
            
            result = session.run(relationships_query, parameters={"relationships": relationships})
        
        # Test complex query with multiple conditions
        import time
        start_time = time.time()
        
        query_params = {
            "entity_types": ["Concept"],
            "properties": {"test_property": "value_2"},
            "relationships": [
                {
                    "type": "REPRESENTS",
                    "direction": "incoming",
                    "target_type": "Symbol"
                }
            ],
            "filters": {
                "name": {"operator": "CONTAINS", "value": "Concept"}
            },
            "pagination": {"skip": 0, "limit": 100}
        }
        
        results = self.client.query_knowledge_graph(query_params)
        
        # Verify correct results
        self.assertGreaterEqual(len(results["entities"]), 1)
        
        # Check performance
        query_time = results.get("query_time", 0)
        self.assertLess(query_time, 1.0)  # Should be reasonably fast, adjust threshold as needed

    def test_concurrent_operations(self):
        """Test concurrent operations on the Neo4j instance."""
        # This is a basic test that simulates concurrent operations
        # For a more comprehensive test, consider using threading or multiprocessing
        
        # Create base entities
        concept_properties = {
            "name": "Concurrent Concept",
            "description": "A concept for concurrent testing",
            "knowledge_tier": KnowledgeTier.L1.value
        }
        
        concept_id = self.client.create_entity(
            entity_type="Concept",
            properties=concept_properties,
            provenance={"source": "integration_test"}
        )
        
        # Simulate concurrent operations by creating multiple symbols
        # and relationships to the same concept
        symbol_ids = []
        for i in range(5):
            symbol_properties = {
                "name": f"Concurrent Symbol {i}",
                "latex": f"\\beta_{i}",
                "context": "Concurrent Context",
                "knowledge_tier": KnowledgeTier.L1.value
            }
            
            symbol_id = self.client.create_entity(
                entity_type="Symbol",
                properties=symbol_properties,
                provenance={"source": "integration_test"}
            )
            symbol_ids.append(symbol_id)
            
            # Create relationship
            relationship_properties = {
                "context": f"Concurrent Relationship {i}",
                "confidence": 0.9
            }
            
            self.client.create_relationship(
                from_entity_id=symbol_id,
                relationship_type="REPRESENTS",
                to_entity_id=concept_id,
                properties=relationship_properties
            )
        
        # Query to verify all relationships were created
        query_params = {
            "entity_types": ["Concept"],
            "properties": {"name": "Concurrent Concept"},
            "relationships": [
                {
                    "type": "REPRESENTS",
                    "direction": "incoming"
                }
            ]
        }
        
        results = self.client.query_knowledge_graph(query_params)
        
        # Should find the concept and all symbols
        symbol_count = 0
        relationship_count = 0
        
        for entity in results["entities"]:
            if "latex" in entity:  # It's a symbol
                symbol_count += 1
        
        for relationship in results["relationships"]:
            if relationship.get("context", "").startswith("Concurrent Relationship"):
                relationship_count += 1
        
        self.assertEqual(symbol_count, 5)
        self.assertEqual(relationship_count, 5)


if __name__ == "__main__":
    unittest.main()
