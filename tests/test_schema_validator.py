"""
Tests for the schema validator module.
"""

import unittest
from datetime import datetime

from knowledge_storage_mcp.schema.validator import SchemaValidator


class TestSchemaValidator(unittest.TestCase):
    """Test cases for the SchemaValidator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.validator = SchemaValidator(enabled=True)
        self.validator_disabled = SchemaValidator(enabled=False)
        self.current_time = datetime.utcnow().isoformat() + "Z"
        
    def test_validate_entity_concept(self):
        """Test validating a Concept entity."""
        # Valid concept entity
        concept_properties = {
            "id": "concept-123",
            "name": "Orthogonal Collocation",
            "description": "A numerical method for solving differential equations",
            "domain": "Numerical Analysis",
            "knowledge_tier": "L2",
            "created_at": self.current_time,
            "updated_at": self.current_time
        }
        
        # Validation should succeed
        validated = self.validator.validate_entity("Concept", concept_properties)
        self.assertEqual(validated["name"], "Orthogonal Collocation")
        self.assertEqual(validated["knowledge_tier"], "L2")
        
        # With validation disabled, it should return the original properties
        validated_disabled = self.validator_disabled.validate_entity("Concept", concept_properties)
        self.assertEqual(validated_disabled, concept_properties)
        
    def test_validate_entity_symbol(self):
        """Test validating a Symbol entity."""
        # Valid symbol entity
        symbol_properties = {
            "id": "symbol-123",
            "name": "alpha",
            "latex": "\\alpha",
            "context": "Heat Transfer Coefficient",
            "meaning": "Heat transfer coefficient in W/(m²·K)",
            "dimensions": "W/(m²·K)",
            "knowledge_tier": "L1",
            "created_at": self.current_time,
            "updated_at": self.current_time
        }
        
        # Validation should succeed
        validated = self.validator.validate_entity("Symbol", symbol_properties)
        self.assertEqual(validated["name"], "alpha")
        self.assertEqual(validated["latex"], "\\alpha")
        
    def test_validate_entity_missing_required(self):
        """Test validating an entity with missing required fields."""
        # Symbol entity missing required fields
        symbol_properties = {
            "id": "symbol-123",
            "name": "alpha",
            # Missing latex and context
            "meaning": "Heat transfer coefficient",
            "created_at": self.current_time,
            "updated_at": self.current_time
        }
        
        # Validation should fail
        with self.assertRaises(ValueError):
            self.validator.validate_entity("Symbol", symbol_properties)
            
        # With validation disabled, it should return the original properties
        validated_disabled = self.validator_disabled.validate_entity("Symbol", symbol_properties)
        self.assertEqual(validated_disabled, symbol_properties)
        
    def test_validate_entity_unknown_type(self):
        """Test validating an entity with unknown type."""
        # Entity with unknown type
        entity_properties = {
            "id": "unknown-123",
            "name": "Unknown Entity",
            "created_at": self.current_time,
            "updated_at": self.current_time
        }
        
        # Validation should fail
        with self.assertRaises(ValueError):
            self.validator.validate_entity("UnknownType", entity_properties)
            
        # With validation disabled, it should return the original properties
        validated_disabled = self.validator_disabled.validate_entity("UnknownType", entity_properties)
        self.assertEqual(validated_disabled, entity_properties)
        
    def test_validate_relationship_represents(self):
        """Test validating a REPRESENTS relationship."""
        # Valid REPRESENTS relationship
        relationship_properties = {
            "id": "rel-123",
            "context": "Heat Transfer Equations",
            "confidence": 0.95,
            "created_at": self.current_time
        }
        
        # Validation should succeed
        validated = self.validator.validate_relationship(
            "REPRESENTS", "Symbol", "Concept", relationship_properties
        )
        self.assertEqual(validated["context"], "Heat Transfer Equations")
        self.assertEqual(validated["confidence"], 0.95)
        
        # With validation disabled, it should return the original properties
        validated_disabled = self.validator_disabled.validate_relationship(
            "REPRESENTS", "Symbol", "Concept", relationship_properties
        )
        self.assertEqual(validated_disabled, relationship_properties)
        
    def test_validate_relationship_invalid_entities(self):
        """Test validating a relationship with invalid entity types."""
        # Valid relationship properties
        relationship_properties = {
            "id": "rel-123",
            "context": "Heat Transfer Equations",
            "confidence": 0.95,
            "created_at": self.current_time
        }
        
        # Validation should fail because REPRESENTS is only valid between Symbol and Concept
        with self.assertRaises(ValueError):
            self.validator.validate_relationship(
                "REPRESENTS", "Concept", "Symbol", relationship_properties
            )
            
        # With validation disabled, it should return the original properties
        validated_disabled = self.validator_disabled.validate_relationship(
            "REPRESENTS", "Concept", "Symbol", relationship_properties
        )
        self.assertEqual(validated_disabled, relationship_properties)
        
    def test_validate_relationship_missing_required(self):
        """Test validating a relationship with missing required fields."""
        # Relationship missing required fields
        relationship_properties = {
            "id": "rel-123",
            # Missing context
            "confidence": 0.95,
            "created_at": self.current_time
        }
        
        # Validation should fail
        with self.assertRaises(ValueError):
            self.validator.validate_relationship(
                "REPRESENTS", "Symbol", "Concept", relationship_properties
            )
            
        # With validation disabled, it should return the original properties
        validated_disabled = self.validator_disabled.validate_relationship(
            "REPRESENTS", "Symbol", "Concept", relationship_properties
        )
        self.assertEqual(validated_disabled, relationship_properties)
        
    def test_validate_relationship_unknown_type(self):
        """Test validating a relationship with unknown type."""
        # Relationship with unknown type
        relationship_properties = {
            "id": "rel-123",
            "created_at": self.current_time
        }
        
        # Validation should fail
        with self.assertRaises(ValueError):
            self.validator.validate_relationship(
                "UNKNOWN_TYPE", "Symbol", "Concept", relationship_properties
            )
            
        # With validation disabled, it should return the original properties
        validated_disabled = self.validator_disabled.validate_relationship(
            "UNKNOWN_TYPE", "Symbol", "Concept", relationship_properties
        )
        self.assertEqual(validated_disabled, relationship_properties)
        
    def test_check_entity_compatibility(self):
        """Test checking entity compatibility."""
        # Valid concept entity
        concept_properties = {
            "id": "concept-123",
            "name": "Orthogonal Collocation",
            "description": "A numerical method for solving differential equations",
            "domain": "Numerical Analysis",
            "knowledge_tier": "L2",
            "created_at": self.current_time,
            "updated_at": self.current_time
        }
        
        # Invalid concept entity (missing name)
        invalid_concept_properties = {
            "id": "concept-123",
            # Missing name
            "description": "A numerical method for solving differential equations",
            "domain": "Numerical Analysis",
            "knowledge_tier": "L2",
            "created_at": self.current_time,
            "updated_at": self.current_time
        }
        
        # Check compatibility for valid entity
        is_valid, error = self.validator.check_entity_compatibility("Concept", concept_properties)
        self.assertTrue(is_valid)
        self.assertIsNone(error)
        
        # Check compatibility for invalid entity
        is_valid, error = self.validator.check_entity_compatibility("Concept", invalid_concept_properties)
        self.assertFalse(is_valid)
        self.assertIsNotNone(error)
        
        # With validation disabled, it should always return valid
        is_valid, error = self.validator_disabled.check_entity_compatibility("Concept", invalid_concept_properties)
        self.assertTrue(is_valid)
        self.assertIsNone(error)
        
    def test_check_relationship_compatibility(self):
        """Test checking relationship compatibility."""
        # Valid REPRESENTS relationship
        relationship_properties = {
            "id": "rel-123",
            "context": "Heat Transfer Equations",
            "confidence": 0.95,
            "created_at": self.current_time
        }
        
        # Invalid REPRESENTS relationship (missing context)
        invalid_relationship_properties = {
            "id": "rel-123",
            # Missing context
            "confidence": 0.95,
            "created_at": self.current_time
        }
        
        # Check compatibility for valid relationship
        is_valid, error = self.validator.check_relationship_compatibility(
            "REPRESENTS", "Symbol", "Concept", relationship_properties
        )
        self.assertTrue(is_valid)
        self.assertIsNone(error)
        
        # Check compatibility for invalid relationship
        is_valid, error = self.validator.check_relationship_compatibility(
            "REPRESENTS", "Symbol", "Concept", invalid_relationship_properties
        )
        self.assertFalse(is_valid)
        self.assertIsNotNone(error)
        
        # Check compatibility for invalid entity types
        is_valid, error = self.validator.check_relationship_compatibility(
            "REPRESENTS", "Concept", "Symbol", relationship_properties
        )
        self.assertFalse(is_valid)
        self.assertIsNotNone(error)
        
        # With validation disabled, it should always return valid
        is_valid, error = self.validator_disabled.check_relationship_compatibility(
            "REPRESENTS", "Concept", "Symbol", invalid_relationship_properties
        )
        self.assertTrue(is_valid)
        self.assertIsNone(error)


if __name__ == "__main__":
    unittest.main()
