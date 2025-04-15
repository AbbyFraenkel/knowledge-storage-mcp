"""
Relationship types schema for Knowledge Storage MCP.

This module defines the relationship types for the knowledge graph, including their
properties, constraints, and validation rules. Relationship types are designed to
connect entities in meaningful ways while supporting the architectural principles.
"""

from enum import Enum
from typing import Dict, Any, List, Optional, Tuple
from pydantic import BaseModel, Field, validator

# Base relationship model
class BaseRelationshipType(BaseModel):
    """Base model for all relationship types."""
    
    id: str = Field(..., description="Unique identifier for the relationship")
    created_at: str = Field(..., description="ISO timestamp of creation")
    
    class Config:
        """Pydantic configuration."""
        extra = "allow"  # Allow additional fields

# Define common relationship types
class CommonRelationshipTypes(str, Enum):
    """Common relationship types in the knowledge graph."""
    
    CONTAINS = "CONTAINS"  # Entity contains another entity
    IMPLEMENTS = "IMPLEMENTS"  # Entity implements another entity
    REFERENCES = "REFERENCES"  # Entity references another entity
    IS_A = "IS_A"  # Entity is a type of another entity
    PART_OF = "PART_OF"  # Entity is part of another entity
    DEFINES = "DEFINES"  # Entity defines another entity
    USES = "USES"  # Entity uses another entity
    APPLIES_TO = "APPLIES_TO"  # Entity applies to another entity
    EXTENDS = "EXTENDS"  # Entity extends another entity
    RELATED_TO = "RELATED_TO"  # Generic relationship

# Symbol-Concept Relationships
class SymbolConceptRelationshipType(BaseRelationshipType):
    """
    Symbol-Concept relationship type.
    
    Represents the relationship between a symbol and the concept it represents.
    This is a key relationship type supporting the symbol-concept separation principle.
    """
    
    context: str = Field(..., description="Context in which the symbol represents the concept")
    confidence: float = Field(1.0, description="Confidence level of the relationship (0.0-1.0)")
    
    @validator('confidence')
    def check_confidence(cls, v):
        """Validate confidence is between 0 and 1."""
        if not 0.0 <= v <= 1.0:
            raise ValueError('Confidence must be between 0.0 and 1.0')
        return v
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "rel-123e4567-e89b-12d3-a456-426614174000",
                "context": "Heat Transfer Equations",
                "confidence": 0.95,
                "created_at": "2025-04-15T10:40:00Z"
            }
        }

# Symbol Conflict Relationship
class SymbolConflictRelationshipType(BaseRelationshipType):
    """
    Symbol Conflict relationship type.
    
    Represents a conflict between two symbols, where the same notation
    is used with different meanings in different contexts.
    """
    
    resolution_strategy: str = Field(..., description="Strategy for resolving the conflict")
    canonical_choice: Optional[str] = Field(None, description="Preferred symbol for cross-domain references")
    resolution_notes: Optional[str] = Field(None, description="Notes on conflict resolution")
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "rel-123e4567-e89b-12d3-a456-426614174000",
                "resolution_strategy": "Context disambiguation",
                "canonical_choice": "alpha_thermal",
                "resolution_notes": "Use alpha_thermal for heat transfer and alpha_optical for optics",
                "created_at": "2025-04-15T10:41:00Z"
            }
        }

# Cross-Domain Interpretation Relationship
class DomainInterpretationRelationshipType(BaseRelationshipType):
    """
    Domain Interpretation relationship type.
    
    Represents a domain-specific interpretation of a symbol or concept.
    """
    
    meaning: str = Field(..., description="Domain-specific meaning")
    standard_usage: Optional[str] = Field(None, description="Standard usage in the domain")
    units: Optional[str] = Field(None, description="Domain-specific units")
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "rel-123e4567-e89b-12d3-a456-426614174000",
                "meaning": "Heat transfer coefficient in thermal engineering",
                "standard_usage": "Used in energy balance equations",
                "units": "W/(m²·K)",
                "created_at": "2025-04-15T10:42:00Z"
            }
        }

# Document-Entity Relationship
class DocumentEntityRelationshipType(BaseRelationshipType):
    """
    Document-Entity relationship type.
    
    Represents the relationship between a document and an entity it contains.
    """
    
    section: Optional[str] = Field(None, description="Document section containing the entity")
    page: Optional[int] = Field(None, description="Page number where the entity appears")
    context: Optional[str] = Field(None, description="Context surrounding the entity")
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "rel-123e4567-e89b-12d3-a456-426614174000",
                "section": "3.2 Adaptive Refinement Algorithm",
                "page": 7,
                "context": "The algorithm is introduced after discussing error estimation",
                "created_at": "2025-04-15T10:43:00Z"
            }
        }

# Algorithm-Implementation Relationship
class AlgorithmImplementationRelationshipType(BaseRelationshipType):
    """
    Algorithm-Implementation relationship type.
    
    Represents the relationship between an algorithm and its implementation.
    """
    
    language: str = Field(..., description="Implementation language")
    optimizations: Optional[List[str]] = Field(None, description="Implementation optimizations")
    deviations: Optional[List[str]] = Field(None, description="Deviations from the algorithm specification")
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "rel-123e4567-e89b-12d3-a456-426614174000",
                "language": "Julia",
                "optimizations": ["Parallel processing", "Cache optimization"],
                "deviations": ["Uses simpler error estimation for performance"],
                "created_at": "2025-04-15T10:44:00Z"
            }
        }

# Concept-Concept Relationship
class ConceptConceptRelationshipType(BaseRelationshipType):
    """
    Concept-Concept relationship type.
    
    Represents the relationship between two concepts.
    """
    
    relationship_type: str = Field(..., description="Type of relationship between concepts")
    description: Optional[str] = Field(None, description="Description of the relationship")
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "rel-123e4567-e89b-12d3-a456-426614174000",
                "relationship_type": "GENERALIZES",
                "description": "Spectral methods generalize orthogonal collocation approaches",
                "created_at": "2025-04-15T10:45:00Z"
            }
        }

# Map of relationship types to valid entity type pairs
VALID_RELATIONSHIP_ENTITY_PAIRS: Dict[str, List[Tuple[str, str]]] = {
    "REPRESENTS": [("Symbol", "Concept")],
    "CONFLICTS_WITH": [("Symbol", "Symbol")],
    "HAS_INTERPRETATION_IN": [("Symbol", "Domain"), ("Concept", "Domain")],
    "CONTAINS": [("Document", "Symbol"), ("Document", "Concept"), ("Document", "Algorithm")],
    "IMPLEMENTS": [("Implementation", "Algorithm")],
    "IS_A": [("Concept", "Concept"), ("Algorithm", "Algorithm")],
    "PART_OF": [("Concept", "Concept"), ("Algorithm", "Algorithm")],
    "REFERENCES": [("Document", "Document"), ("Algorithm", "Concept")],
    "DEFINES": [("Document", "Concept"), ("Document", "Algorithm")],
    "USES": [("Algorithm", "Concept"), ("Implementation", "Concept")],
    "GENERALIZES": [("Concept", "Concept")],
    "SPECIALIZES": [("Concept", "Concept")],
    "EXTENDS": [("Algorithm", "Algorithm"), ("Implementation", "Implementation")],
    "RELATED_TO": [("Concept", "Concept"), ("Algorithm", "Algorithm"), ("Implementation", "Implementation")]
}

# Map of relationship types to schema models
RELATIONSHIP_TYPE_SCHEMAS = {
    "REPRESENTS": SymbolConceptRelationshipType,
    "CONFLICTS_WITH": SymbolConflictRelationshipType,
    "HAS_INTERPRETATION_IN": DomainInterpretationRelationshipType,
    "CONTAINS": DocumentEntityRelationshipType,
    "IMPLEMENTS": AlgorithmImplementationRelationshipType,
    "GENERALIZES": ConceptConceptRelationshipType,
    "SPECIALIZES": ConceptConceptRelationshipType,
    "IS_A": BaseRelationshipType,
    "PART_OF": BaseRelationshipType,
    "REFERENCES": BaseRelationshipType,
    "DEFINES": BaseRelationshipType,
    "USES": BaseRelationshipType,
    "EXTENDS": BaseRelationshipType,
    "RELATED_TO": BaseRelationshipType
}

def validate_relationship(
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
        ValueError: If relationship type is not recognized, entity types are not valid,
                   or validation fails
    """
    # Check if relationship type is valid
    if relationship_type not in RELATIONSHIP_TYPE_SCHEMAS:
        raise ValueError(f"Unknown relationship type: {relationship_type}")
        
    # Check if entity types are valid for this relationship
    if relationship_type in VALID_RELATIONSHIP_ENTITY_PAIRS:
        valid_pairs = VALID_RELATIONSHIP_ENTITY_PAIRS[relationship_type]
        if (from_entity_type, to_entity_type) not in valid_pairs:
            valid_pairs_str = ", ".join([f"{a}->{b}" for a, b in valid_pairs])
            raise ValueError(
                f"Invalid entity types for {relationship_type}: {from_entity_type}->{to_entity_type}. "
                f"Valid pairs are: {valid_pairs_str}"
            )
    
    # Get schema model for validation
    schema_model = RELATIONSHIP_TYPE_SCHEMAS[relationship_type]
    
    try:
        validated = schema_model(**properties)
        return validated.dict()
    except Exception as e:
        raise ValueError(f"Validation failed for {relationship_type}: {str(e)}")
