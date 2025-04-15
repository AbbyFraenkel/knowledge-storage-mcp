"""
Neo4j constraint and index management for Knowledge Storage MCP.

This module provides functions for creating and managing Neo4j constraints and indices
to ensure data integrity and optimize query performance for the knowledge graph.
"""

from typing import List, Dict, Any, Optional, Callable
from contextlib import contextmanager
import time

from neo4j import Driver, Session, Transaction
from neo4j.exceptions import ConstraintError, ClientError
from loguru import logger

from knowledge_storage_mcp.schema.entity_types import ENTITY_TYPE_SCHEMAS
from knowledge_storage_mcp.schema.relationship_types import VALID_RELATIONSHIP_ENTITY_PAIRS


def create_entity_constraints(driver: Driver) -> None:
    """
    Create entity constraints in Neo4j.
    
    This function creates uniqueness constraints on entity IDs and
    ensures that entity types are valid according to the schema.
    
    Args:
        driver: Neo4j driver instance
    """
    # Create uniqueness constraints for entity IDs by type
    constraints = []
    
    for entity_type in ENTITY_TYPE_SCHEMAS.keys():
        # Create uniqueness constraint on entity ID
        constraint_name = f"{entity_type.lower()}_id_uniqueness"
        constraint_query = f"""
        CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
        FOR (e:{entity_type})
        REQUIRE e.id IS UNIQUE
        """
        constraints.append((constraint_name, constraint_query))
        
        # Create existence constraint on required properties
        # This ensures that required properties must exist
        constraint_name = f"{entity_type.lower()}_required_properties"
        constraint_query = f"""
        CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
        FOR (e:{entity_type})
        REQUIRE e.name IS NOT NULL AND e.created_at IS NOT NULL
        """
        constraints.append((constraint_name, constraint_query))
    
    # Create any additional entity type-specific constraints
    # Symbol entity should have latex and context properties
    constraint_name = "symbol_required_properties"
    constraint_query = """
    CREATE CONSTRAINT symbol_required_properties IF NOT EXISTS
    FOR (e:Symbol)
    REQUIRE e.latex IS NOT NULL AND e.context IS NOT NULL
    """
    constraints.append((constraint_name, constraint_query))
    
    # Execute all constraint creation queries
    with driver.session() as session:
        for name, query in constraints:
            try:
                session.run(query)
                logger.info(f"Created constraint: {name}")
            except ClientError as e:
                logger.warning(f"Error creating constraint {name}: {str(e)}")


def create_entity_indices(driver: Driver) -> None:
    """
    Create entity indices in Neo4j.
    
    This function creates indices on entity properties to optimize
    query performance for common query patterns.
    
    Args:
        driver: Neo4j driver instance
    """
    # Create indices for common query patterns
    indices = []
    
    # Common indices across all entity types
    for entity_type in ENTITY_TYPE_SCHEMAS.keys():
        # Create index on entity name
        index_name = f"{entity_type.lower()}_name_index"
        index_query = f"""
        CREATE INDEX {index_name} IF NOT EXISTS
        FOR (e:{entity_type})
        ON (e.name)
        """
        indices.append((index_name, index_query))
        
        # Create index on knowledge tier
        index_name = f"{entity_type.lower()}_knowledge_tier_index"
        index_query = f"""
        CREATE INDEX {index_name} IF NOT EXISTS
        FOR (e:{entity_type})
        ON (e.knowledge_tier)
        """
        indices.append((index_name, index_query))
        
        # Create index on creation timestamp
        index_name = f"{entity_type.lower()}_created_at_index"
        index_query = f"""
        CREATE INDEX {index_name} IF NOT EXISTS
        FOR (e:{entity_type})
        ON (e.created_at)
        """
        indices.append((index_name, index_query))
    
    # Entity-specific indices
    
    # Symbol entity - latex index
    index_name = "symbol_latex_index"
    index_query = """
    CREATE INDEX symbol_latex_index IF NOT EXISTS
    FOR (e:Symbol)
    ON (e.latex)
    """
    indices.append((index_name, index_query))
    
    # Document entity - year and authors indices
    index_name = "document_year_index"
    index_query = """
    CREATE INDEX document_year_index IF NOT EXISTS
    FOR (e:Document)
    ON (e.year)
    """
    indices.append((index_name, index_query))
    
    index_name = "document_authors_index"
    index_query = """
    CREATE INDEX document_authors_index IF NOT EXISTS
    FOR (e:Document)
    ON (e.authors)
    """
    indices.append((index_name, index_query))
    
    # Concept entity - domain index
    index_name = "concept_domain_index"
    index_query = """
    CREATE INDEX concept_domain_index IF NOT EXISTS
    FOR (e:Concept)
    ON (e.domain)
    """
    indices.append((index_name, index_query))
    
    # Execute all index creation queries
    with driver.session() as session:
        for name, query in indices:
            try:
                session.run(query)
                logger.info(f"Created index: {name}")
            except ClientError as e:
                logger.warning(f"Error creating index {name}: {str(e)}")


def initialize_database(driver: Driver) -> None:
    """
    Initialize Neo4j database with constraints and indices.
    
    This function creates all necessary constraints and indices
    to ensure data integrity and optimize query performance.
    
    Args:
        driver: Neo4j driver instance
    """
    logger.info("Initializing Neo4j database with constraints and indices")
    create_entity_constraints(driver)
    create_entity_indices(driver)
    logger.info("Database initialization complete")


@contextmanager
def transaction(driver: Driver, access_mode="WRITE"):
    """
    Transaction context manager for Neo4j operations.
    
    This context manager provides a transaction object that can be used
    for atomic operations, with automatic commit/rollback handling.
    
    Args:
        driver: Neo4j driver instance
        access_mode: Transaction access mode ('READ' or 'WRITE')
    
    Yields:
        Neo4j transaction object
    """
    session = None
    tx = None
    
    try:
        session = driver.session(access_mode=access_mode)
        tx = session.begin_transaction()
        yield tx
        tx.commit()
    except Exception as e:
        logger.error(f"Transaction error: {str(e)}")
        if tx is not None:
            tx.rollback()
        raise
    finally:
        if session is not None:
            session.close()


def batch_operation(driver: Driver, operations: List[Dict[str, Any]], 
                    batch_size: int = 100, 
                    operation_function: Callable = None) -> List[Any]:
    """
    Execute a batch of operations in transactions.
    
    This function executes a batch of operations in multiple transactions,
    with automatic commit/rollback handling.
    
    Args:
        driver: Neo4j driver instance
        operations: List of operation parameters
        batch_size: Number of operations per transaction
        operation_function: Function to execute for each operation
    
    Returns:
        List of operation results
    """
    if operation_function is None:
        raise ValueError("Operation function must be provided")
    
    results = []
    batches = [operations[i:i + batch_size] for i in range(0, len(operations), batch_size)]
    
    for batch in batches:
        with transaction(driver) as tx:
            batch_results = []
            for operation in batch:
                result = operation_function(tx, **operation)
                batch_results.append(result)
            results.extend(batch_results)
    
    return results
