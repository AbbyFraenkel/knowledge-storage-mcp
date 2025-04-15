"""
Query optimizer for Neo4j client.

This module provides optimization strategies for Neo4j queries,
improving performance for large knowledge graphs.
"""

from typing import Dict, List, Any, Optional, Tuple
from loguru import logger


class QueryOptimizer:
    """
    Query optimizer for Neo4j client.
    
    This class provides methods for optimizing Cypher queries,
    implementing indexing strategies, and improving performance
    for large knowledge graphs.
    """
    
    def __init__(self):
        """Initialize query optimizer."""
        self.index_hints = {
            "Document": ["id", "title", "year"],
            "Concept": ["id", "name", "domain"],
            "Symbol": ["id", "name", "latex", "context"],
            "Algorithm": ["id", "name", "complexity"],
            "Implementation": ["id", "name", "language"],
            "Domain": ["id", "name"]
        }
    
    def create_indices(self, session) -> None:
        """
        Create indices for entity properties to improve query performance.
        
        Args:
            session: Neo4j session
        """
        for entity_type, properties in self.index_hints.items():
            for prop in properties:
                try:
                    # Create index if it doesn't exist
                    # Neo4j 5+ syntax
                    query = f"""
                    CREATE INDEX IF NOT EXISTS FOR (n:{entity_type}) ON (n.{prop})
                    """
                    session.run(query)
                    logger.info(f"Created or verified index on {entity_type}.{prop}")
                except Exception as e:
                    logger.error(f"Error creating index on {entity_type}.{prop}: {str(e)}")
    
    def optimize_query(self, query_params: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        Optimize a query based on the provided parameters.
        
        Args:
            query_params: Query parameters
            
        Returns:
            Tuple of (optimized_query, parameters)
        """
        # Extract query parameters
        entity_types = query_params.get("entity_types", [])
        properties = query_params.get("properties", {})
        relationships = query_params.get("relationships", [])
        filters = query_params.get("filters", {})
        pagination = query_params.get("pagination", {"skip": 0, "limit": 100})
        
        # Build optimized Cypher query
        query_parts = []
        where_clauses = []
        return_clauses = ["e"]
        parameters = {}
        
        # Start with the most selective patterns first
        # If we have specific properties, use those in the initial MATCH
        if properties and entity_types:
            # Find the most selective property based on index hints
            selective_props = []
            for entity_type in entity_types:
                if entity_type in self.index_hints:
                    for prop in properties:
                        if prop in self.index_hints[entity_type]:
                            selective_props.append((entity_type, prop))
            
            if selective_props:
                # Use the most selective property in the initial MATCH
                entity_type, prop = selective_props[0]
                value = properties[prop]
                entity_label = ":".join(entity_types)
                
                query_parts.append(f"MATCH (e:{entity_label} {{{prop}: ${prop}}})")
                parameters[prop] = value
                
                # Remove this property from further WHERE clauses
                properties_copy = properties.copy()
                properties_copy.pop(prop)
                
                # Add remaining properties to WHERE clause
                for key, value in properties_copy.items():
                    where_clauses.append(f"e.{key} = ${key}")
                    parameters[key] = value
            else:
                # No selective properties, use standard MATCH with entity types
                entity_labels = ":".join(entity_types)
                query_parts.append(f"MATCH (e:{entity_labels})")
                
                # Add all properties to WHERE clause
                for key, value in properties.items():
                    where_clauses.append(f"e.{key} = ${key}")
                    parameters[key] = value
        elif entity_types:
            # Use entity types in MATCH
            entity_labels = ":".join(entity_types)
            query_parts.append(f"MATCH (e:{entity_labels})")
        else:
            # No entity types specified, use general MATCH
            query_parts.append("MATCH (e)")
                
        # Handle filters
        if filters:
            for key, value in filters.items():
                if isinstance(value, dict):
                    # Handle operators like >, <, >=, <=
                    operator = value.get("operator", "=")
                    filter_value = value.get("value")
                    if filter_value is not None:
                        where_clauses.append(f"e.{key} {operator} ${key}")
                        parameters[key] = filter_value
                else:
                    where_clauses.append(f"e.{key} = ${key}")
                    parameters[key] = value
        
        # Handle relationships - use separate MATCH clauses for better performance
        if relationships:
            for i, rel in enumerate(relationships):
                rel_type = rel.get("type", "")
                direction = rel.get("direction", "outgoing")
                target_label = rel.get("target_type", "")
                
                # Build relationship pattern
                if direction == "outgoing":
                    pattern = f"MATCH (e)-[r{i}:{rel_type}]->(related{i}"
                elif direction == "incoming":
                    pattern = f"MATCH (e)<-[r{i}:{rel_type}]-(related{i}"
                else:  # bidirectional
                    pattern = f"MATCH (e)-[r{i}:{rel_type}]-(related{i}"
                    
                # Add target label if specified
                if target_label:
                    pattern += f":{target_label}"
                    
                pattern += ")"
                query_parts.append(pattern)
                return_clauses.append(f"r{i}")
                return_clauses.append(f"related{i}")
        
        # Add WHERE clause if needed
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
            
        # Add RETURN clause - include DISTINCT for better performance
        query_parts.append("RETURN DISTINCT " + ", ".join(return_clauses))
        
        # Add efficient pagination
        # For large result sets, use WHERE clause with IDs instead of SKIP
        skip = pagination.get("skip", 0)
        limit = pagination.get("limit", 100)
        
        # Add a reasonable LIMIT clause to prevent excessive memory usage
        max_limit = 1000  # Maximum number of records to return
        effective_limit = min(limit, max_limit)
        
        if skip > 0:
            query_parts.append(f"SKIP {skip} LIMIT {effective_limit}")
        else:
            query_parts.append(f"LIMIT {effective_limit}")
        
        # Join query parts
        query = "\n".join(query_parts)
        
        return query, parameters
    
    def get_indexing_statements(self) -> List[str]:
        """
        Get Cypher statements for creating all recommended indices.
        
        Returns:
            List of Cypher statements for index creation
        """
        statements = []
        
        for entity_type, properties in self.index_hints.items():
            for prop in properties:
                # Neo4j 5+ syntax
                statement = f"""
                CREATE INDEX IF NOT EXISTS FOR (n:{entity_type}) ON (n.{prop})
                """
                statements.append(statement)
                
        return statements
    
    def generate_query_explanation(self, query: str, parameters: Dict[str, Any], session) -> str:
        """
        Generate explanation for a query to understand its execution plan.
        
        Args:
            query: Cypher query
            parameters: Query parameters
            session: Neo4j session
            
        Returns:
            Query explanation
        """
        try:
            explanation_query = f"EXPLAIN {query}"
            result = session.run(explanation_query, parameters=parameters)
            summary = result.consume()
            
            # Format the explanation
            explanation = f"Query Plan for: {query}\n\n"
            explanation += f"Estimated rows: {summary.counters}\n"
            
            return explanation
        except Exception as e:
            logger.error(f"Error generating query explanation: {str(e)}")
            return f"Error generating explanation: {str(e)}"
    
    def analyze_query(self, query: str, parameters: Dict[str, Any], session) -> Dict[str, Any]:
        """
        Analyze a query to get performance metrics.
        
        Args:
            query: Cypher query
            parameters: Query parameters
            session: Neo4j session
            
        Returns:
            Performance metrics
        """
        try:
            # Use PROFILE to get detailed execution metrics
            profile_query = f"PROFILE {query}"
            result = session.run(profile_query, parameters=parameters)
            summary = result.consume()
            
            # Extract performance metrics
            metrics = {
                "execution_time_ms": summary.result_available_after,
                "records_affected": summary.counters.contains_updates and summary.counters.nodes_created or 0,
                "db_hits": getattr(summary, "db_hits", 0),
                "rows": getattr(summary, "result_available_after", 0)
            }
            
            return metrics
        except Exception as e:
            logger.error(f"Error analyzing query: {str(e)}")
            return {"error": str(e)}
    
    def suggest_query_improvements(self, metrics: Dict[str, Any]) -> List[str]:
        """
        Suggest improvements based on query performance metrics.
        
        Args:
            metrics: Performance metrics
            
        Returns:
            List of improvement suggestions
        """
        suggestions = []
        
        # Check execution time
        if metrics.get("execution_time_ms", 0) > 1000:
            suggestions.append("Query is slow (>1000ms). Consider adding more specific indices or refining the query.")
        
        # Check database hits
        if metrics.get("db_hits", 0) > 10000:
            suggestions.append("High number of database hits. Consider adding more selective filters or indices.")
        
        # General suggestions if no specific issues found
        if not suggestions:
            suggestions.append("Query performance is acceptable. Consider batch processing for large operations.")
        
        return suggestions
