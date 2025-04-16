"""
Integration tests for Knowledge Storage MCP.

These tests verify the interaction with a real Neo4j database.
They require a running Neo4j instance to execute successfully.
"""

import os
import unittest
import time
from datetime import datetime

import pytest
from neo4j import GraphDatabase

from knowledge_storage_mcp.db.neo4j_client import Neo4jClient
from knowledge_storage_mcp.schema.validator import SchemaValidator


# Skip these tests if NEO4J_TEST_URI environment variable is not set
SKIP_REASON = "Neo4j test instance not available"
NEO4J_TEST_URI = os.environ.get("NEO4J_TEST_URI", "bolt://localhost:7687")
NEO4J_TEST_USER = os.environ.get("NEO4J_TEST_USER", "neo4j")
NEO4J_TEST_PASSWORD = os.environ.get("NEO4J_TEST_PASSWORD", "password")


@pytest.mark.integration
class TestNeo4jIntegration(unittest.TestCase):
    """Integration tests for Neo4j client."""
    
    @classmethod
    def setUpClass(cls):
        """Set up Neo4j client for all tests."""
        try:
            cls.client = Neo4jClient(
                uri=NEO4J_TEST_URI,
                user=NEO4J_TEST_USER,
                password=NEO4J_TEST_PASSWORD
            )
            # Verify connection
            if not cls.client.check_connection():
                pytest.skip(SKIP_REASON)
                
            # Set up validator
            cls.validator = SchemaValidator(enabled=True)
            
            # Clean up database
            cls._clean_database()
        except Exception as e:
            pytest.skip(f"{SKIP_REASON}: {str(e)}")
    
    @classmethod
    def tearDownClass(cls):
        """Close Neo4j client."""
        try:
            cls._clean_database()
            cls.client.close()
        except:
            pass
    
    @classmethod
    def _clean_database(cls):
        """Remove all test data from database."""
        with cls.client.driver.session() as session:
            # Delete all relationships and then all nodes
            session.run("MATCH ()-[r]-() DELETE r")
            session.run("MATCH (n) DELETE n")
    
    def setUp(self):
        """Set up each test."""
        self.current_time = datetime.utcnow().isoformat() + "Z"
    
    def test_entity_creation_retrieval(self):
        """Test entity creation and retrieval."""
        # Create a test entity
        concept_properties = {
            "name": "Test Concept",
            "description": "A test concept for integration testing",
            "domain": "Testing",
            "knowledge_tier": "L1",
            "created_at": self.current_time,
            "updated_at": self.current_time
        }
        
        entity_id = self.client.create_entity(
            entity_type="Concept",
            properties=concept_properties,
            provenance={"source": "integration_test"}
        )
        
        # Verify entity exists
        self.assertTrue(self.client.entity_exists(entity_id))
        
        # Query entity
        query_params = {
            "entity_types": ["Concept"],
            "properties": {"name": "Test Concept"}
        }
        
        result = self.client.query_knowledge_graph(query_params)
        
        # Verify query results
        self.assertGreaterEqual(len(result["entities"]), 1)
        found = False
        for entity in result["entities"]:
            if entity["id"] == entity_id:
                found = True
                self.assertEqual(entity["name"], "Test Concept")
                self.assertEqual(entity["domain"], "Testing")
                break
                
        self.assertTrue(found, "Created entity not found in query results")
    
    def test_relationship_creation_querying(self):
        """Test relationship creation and querying."""
        # Create two test entities
        symbol_properties = {
            "name": "alpha",
            "latex": "\\alpha",
            "context": "Test Context",
            "created_at": self.current_time,
            "updated_at": self.current_time
        }
        
        concept_properties = {
            "name": "Test Concept",
            "description": "A test concept for relationship testing",
            "domain": "Testing",
            "knowledge_tier": "L1",
            "created_at": self.current_time,
            "updated_at": self.current_time
        }
        
        symbol_id = self.client.create_entity(
            entity_type="Symbol",
            properties=symbol_properties,
            provenance={"source": "integration_test"}
        )
        
        concept_id = self.client.create_entity(
            entity_type="Concept",
            properties=concept_properties,
            provenance={"source": "integration_test"}
        )
        
        # Create relationship
        relationship_properties = {
            "context": "Test Relationship Context",
            "confidence": 0.95,
            "created_at": self.current_time
        }
        
        relationship_id = self.client.create_relationship(
            from_entity_id=symbol_id,
            relationship_type="REPRESENTS",
            to_entity_id=concept_id,
            properties=relationship_properties
        )
        
        # Query relationship
        query_params = {
            "entity_types": ["Symbol"],
            "relationships": [
                {
                    "type": "REPRESENTS",
                    "direction": "outgoing",
                    "target_type": "Concept"
                }
            ]
        }
        
        result = self.client.query_knowledge_graph(query_params)
        
        # Verify query results
        self.assertGreaterEqual(len(result["entities"]), 1)
        self.assertGreaterEqual(len(result["relationships"]), 1)
        
        found = False
        for relationship in result["relationships"]:
            if relationship["id"] == relationship_id:
                found = True
                self.assertEqual(relationship["context"], "Test Relationship Context")
                self.assertEqual(relationship["confidence"], 0.95)
                break
                
        self.assertTrue(found, "Created relationship not found in query results")
    
    def test_query_performance(self):
        """Test query performance with multiple entities."""
        # Create multiple test entities
        batch_size = 100
        concept_ids = []
        
        # Create batch of concepts
        start_time = time.time()
        for i in range(batch_size):
            concept_properties = {
                "name": f"Performance Test Concept {i}",
                "description": f"Test concept #{i} for performance testing",
                "domain": "Performance Testing",
                "knowledge_tier": "L1",
                "created_at": self.current_time,
                "updated_at": self.current_time
            }
            
            concept_id = self.client.create_entity(
                entity_type="Concept",
                properties=concept_properties,
                provenance={"source": "performance_test"}
            )
            concept_ids.append(concept_id)
        
        creation_time = time.time() - start_time
        print(f"Created {batch_size} concepts in {creation_time:.2f} seconds")
        
        # Query with pagination
        page_size = 10
        total_results = 0
        start_time = time.time()
        
        for page in range(10):  # Test first 10 pages
            query_params = {
                "entity_types": ["Concept"],
                "properties": {"domain": "Performance Testing"},
                "pagination": {"skip": page * page_size, "limit": page_size}
            }
            
            result = self.client.query_knowledge_graph(query_params)
            total_results += len(result["entities"])
            
            # Verify page size
            self.assertLessEqual(len(result["entities"]), page_size)
            
        query_time = time.time() - start_time
        print(f"Queried {total_results} concepts in {query_time:.2f} seconds")
        
        # Verify all entities were found
        self.assertEqual(total_results, min(batch_size, 100))


if __name__ == "__main__":
    unittest.main()
