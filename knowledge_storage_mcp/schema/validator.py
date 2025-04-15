"""
Schema validator for Knowledge Storage MCP.

This module provides validation functions for entities and relationships
against their schema definitions.
"""

from typing import Dict, Any, Tuple, Optional

from loguru import logger

from knowledge_storage_mcp.schema.entity_types import validate_entity
from knowledge_storage_mcp.schema.relationship_types import validate_relationship


class SchemaValidator:
    """
    Schema validator for Knowledge Storage MCP.
    
    This class provides methods for validating entities and relationships
    against their schema definitions.
    """
    
    def __init__(self, enabled: bool = True):
        """
        Initialize schema validator.
        
        Args:
            enabled: Whether validation is enabled
        """
        self.enabled = enabled
        
    def validate_entity(self, entity_type: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate entity properties against schema.
        
        Args:
            entity_type: Type of entity
            properties: Entity properties
            
        Returns:
            Validated properties
            
        Raises:
            ValueError: If validation is enabled and entity type is not recognized or validation fails
        """
        if not self.enabled:
            logger.debug(f"Schema validation disabled, skipping validation for {entity_type}")
            return properties
            
        try:
            return validate_entity(entity_type, properties)
        except ValueError as e:
            logger.error(f"Entity validation error: {str(e)}")
            raise
            
    def validate_relationship(
        self,
        relationship_type: str,
        from_entity_type: str,
        to_entity_type: str,
        properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate relationship properties against schema.
        
        Args:
            relationship_type: Type of relationship
            from_entity_type: Type of source entity
            to_entity_type: Type of target entity
            properties: Relationship properties
            
        Returns:
            Validated properties
            
        Raises:
            ValueError: If validation is enabled and relationship type is not recognized,
                       entity types are not valid, or validation fails
        """
        if not self.enabled:
            logger.debug(f"Schema validation disabled, skipping validation for {relationship_type}")
            return properties
            
        try:
            return validate_relationship(
                relationship_type, 
                from_entity_type,
                to_entity_type,
                properties
            )
        except ValueError as e:
            logger.error(f"Relationship validation error: {str(e)}")
            raise
            
    def check_entity_compatibility(
        self, 
        entity_type: str, 
        properties: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if entity properties are compatible with schema without raising exceptions.
        
        Args:
            entity_type: Type of entity
            properties: Entity properties
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not self.enabled:
            return True, None
            
        try:
            validate_entity(entity_type, properties)
            return True, None
        except ValueError as e:
            return False, str(e)
            
    def check_relationship_compatibility(
        self,
        relationship_type: str,
        from_entity_type: str,
        to_entity_type: str,
        properties: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if relationship properties are compatible with schema without raising exceptions.
        
        Args:
            relationship_type: Type of relationship
            from_entity_type: Type of source entity
            to_entity_type: Type of target entity
            properties: Relationship properties
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not self.enabled:
            return True, None
            
        try:
            validate_relationship(
                relationship_type, 
                from_entity_type,
                to_entity_type,
                properties
            )
            return True, None
        except ValueError as e:
            return False, str(e)
