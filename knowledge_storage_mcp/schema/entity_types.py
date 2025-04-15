"""
Entity types schema for Knowledge Storage MCP.

This module defines the entity types for the knowledge graph, including their
properties, constraints, and validation rules. Entity types are designed to support
the symbol-concept separation principle and tiered knowledge organization.
"""

from enum import Enum
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, validator

# Define knowledge tiers
class KnowledgeTier(str, Enum):
    """Knowledge tier levels."""
    
    L1 = "L1"  # Core concepts (100-200 words)
    L2 = "L2"  # Functional details (500-1000 words)
    L3 = "L3"  # Complete knowledge (2000+ words)

# Base entity model
class BaseEntityType(BaseModel):
    """Base model for all entity types."""
    
    id: str = Field(..., description="Unique identifier for the entity")
    name: str = Field(..., description="Primary name or identifier for the entity")
    description: Optional[str] = Field(None, description="Brief description of the entity")
    knowledge_tier: KnowledgeTier = Field(KnowledgeTier.L1, description="Knowledge tier level")
    created_at: str = Field(..., description="ISO timestamp of creation")
    updated_at: str = Field(..., description="ISO timestamp of last update")
    
    class Config:
        """Pydantic configuration."""
        extra = "allow"  # Allow additional fields

# Document entity
class DocumentEntity(BaseEntityType):
    """
    Document entity type.
    
    Represents an academic document such as a paper, book, or article.
    """
    
    title: str = Field(..., description="Document title")
    authors: List[str] = Field(..., description="List of document authors")
    year: Optional[int] = Field(None, description="Publication year")
    doi: Optional[str] = Field(None, description="Digital Object Identifier")
    url: Optional[str] = Field(None, description="URL to the document")
    keywords: Optional[List[str]] = Field(None, description="Keywords associated with the document")
    abstract: Optional[str] = Field(None, description="Document abstract")
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "doc-123e4567-e89b-12d3-a456-426614174000",
                "name": "Adaptive Orthogonal Collocation Methods for PDEs",
                "title": "Adaptive Orthogonal Collocation Methods for Partial Differential Equations",
                "authors": ["Smith, J.R.", "Johnson, A.B."],
                "year": 2022,
                "doi": "10.1234/journal.2022.001",
                "knowledge_tier": "L1",
                "created_at": "2025-04-15T10:30:00Z",
                "updated_at": "2025-04-15T10:30:00Z"
            }
        }

# Mathematical concept entity
class ConceptEntity(BaseEntityType):
    """
    Concept entity type.
    
    Represents a mathematical concept, idea, or theory.
    """
    
    aliases: Optional[List[str]] = Field(None, description="Alternative names for the concept")
    domain: Optional[str] = Field(None, description="Mathematical domain (e.g., algebra, calculus)")
    formal_definition: Optional[str] = Field(None, description="Formal mathematical definition")
    properties: Optional[List[str]] = Field(None, description="Key properties of the concept")
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "concept-123e4567-e89b-12d3-a456-426614174000",
                "name": "Orthogonal Collocation",
                "description": "A numerical method for solving differential equations",
                "aliases": ["Collocation Method", "Spectral Collocation"],
                "domain": "Numerical Analysis",
                "knowledge_tier": "L2",
                "created_at": "2025-04-15T10:31:00Z",
                "updated_at": "2025-04-15T10:31:00Z"
            }
        }

# Symbol entity
class SymbolEntity(BaseEntityType):
    """
    Symbol entity type.
    
    Represents a mathematical symbol or notation used in academic papers.
    This type is separate from concepts to maintain the symbol-concept separation principle.
    """
    
    latex: str = Field(..., description="LaTeX representation of the symbol")
    context: str = Field(..., description="Context in which the symbol appears")
    paper_reference: Optional[str] = Field(None, description="Reference to the paper where the symbol is used")
    meaning: Optional[str] = Field(None, description="Meaning of the symbol in its context")
    dimensions: Optional[str] = Field(None, description="Physical dimensions or units")
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "symbol-123e4567-e89b-12d3-a456-426614174000",
                "name": "alpha",
                "latex": "\\alpha",
                "context": "Heat Transfer Coefficient",
                "paper_reference": "doc-123e4567-e89b-12d3-a456-426614174000",
                "meaning": "Heat transfer coefficient in W/(m²·K)",
                "dimensions": "W/(m²·K)",
                "knowledge_tier": "L1",
                "created_at": "2025-04-15T10:32:00Z",
                "updated_at": "2025-04-15T10:32:00Z"
            }
        }

# Algorithm entity
class AlgorithmEntity(BaseEntityType):
    """
    Algorithm entity type.
    
    Represents a mathematical algorithm or computational method.
    """
    
    steps: Optional[List[str]] = Field(None, description="Algorithm steps")
    complexity: Optional[str] = Field(None, description="Computational complexity")
    inputs: Optional[List[str]] = Field(None, description="Algorithm inputs")
    outputs: Optional[List[str]] = Field(None, description="Algorithm outputs")
    pseudo_code: Optional[str] = Field(None, description="Pseudo-code representation")
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "algo-123e4567-e89b-12d3-a456-426614174000",
                "name": "Adaptive HP-Refinement Algorithm",
                "description": "Algorithm for adaptive hp-refinement in spectral element methods",
                "steps": [
                    "Compute solution on initial mesh",
                    "Estimate error in each element",
                    "Mark elements for refinement",
                    "Apply h-refinement or p-refinement based on smoothness indicator",
                    "Update mesh and repeat"
                ],
                "complexity": "O(N log N)",
                "knowledge_tier": "L2",
                "created_at": "2025-04-15T10:33:00Z",
                "updated_at": "2025-04-15T10:33:00Z"
            }
        }

# Implementation entity
class ImplementationEntity(BaseEntityType):
    """
    Implementation entity type.
    
    Represents an implementation of a mathematical algorithm or method.
    """
    
    language: str = Field(..., description="Programming language")
    code_repository: Optional[str] = Field(None, description="Code repository URL")
    dependencies: Optional[List[str]] = Field(None, description="Implementation dependencies")
    performance_metrics: Optional[Dict[str, Any]] = Field(None, description="Performance metrics")
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "impl-123e4567-e89b-12d3-a456-426614174000",
                "name": "KitchenSink Adaptive Collocation",
                "description": "Implementation of adaptive orthogonal collocation in KitchenSink solver",
                "language": "Julia",
                "code_repository": "https://github.com/user/kitchensink",
                "dependencies": ["Julia 1.8+", "SciML Ecosystem"],
                "knowledge_tier": "L3",
                "created_at": "2025-04-15T10:34:00Z",
                "updated_at": "2025-04-15T10:34:00Z"
            }
        }

# Domain entity
class DomainEntity(BaseEntityType):
    """
    Domain entity type.
    
    Represents a mathematical or application domain.
    """
    
    subdomain_of: Optional[str] = Field(None, description="Parent domain")
    related_domains: Optional[List[str]] = Field(None, description="Related domains")
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "id": "domain-123e4567-e89b-12d3-a456-426614174000",
                "name": "Numerical Analysis",
                "description": "Branch of mathematics concerned with numerical approximation",
                "subdomain_of": "Applied Mathematics",
                "related_domains": ["Computational Mathematics", "Scientific Computing"],
                "knowledge_tier": "L1",
                "created_at": "2025-04-15T10:35:00Z",
                "updated_at": "2025-04-15T10:35:00Z"
            }
        }

# Mapping between entity types and their schema models
ENTITY_TYPE_SCHEMAS = {
    "Document": DocumentEntity,
    "Concept": ConceptEntity,
    "Symbol": SymbolEntity,
    "Algorithm": AlgorithmEntity,
    "Implementation": ImplementationEntity,
    "Domain": DomainEntity
}

def validate_entity(entity_type: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate entity properties against schema.
    
    Args:
        entity_type: Type of entity
        properties: Entity properties
        
    Returns:
        Validated properties
        
    Raises:
        ValueError: If entity type is not recognized or validation fails
    """
    if entity_type not in ENTITY_TYPE_SCHEMAS:
        raise ValueError(f"Unknown entity type: {entity_type}")
        
    schema_model = ENTITY_TYPE_SCHEMAS[entity_type]
    
    try:
        validated = schema_model(**properties)
        return validated.dict()
    except Exception as e:
        raise ValueError(f"Validation failed for {entity_type}: {str(e)}")
