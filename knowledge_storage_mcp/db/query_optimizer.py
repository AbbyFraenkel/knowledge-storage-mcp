"""
Query optimization module for Knowledge Storage MCP.

This module provides advanced query optimization capabilities for Neo4j queries,
including caching, performance monitoring, and hint management.
"""

import time
import re
from collections import OrderedDict
from threading import RLock
from typing import Dict, Any, List, Optional, Union, Tuple

from loguru import logger


class QueryCache:
    """
    LRU cache for Neo4j query results with size limit and TTL.
    
    This class provides thread-safe caching for query results with
    configurable size limits and expiration.
    """
    
    def __init__(self, max_size: int = 100, ttl: int = 3600):
        """
        Initialize query cache.
        
        Args:
            max_size: Maximum number of cached queries
            ttl: Time-to-live in seconds for cache entries
        """
        self.max_size = max_size
        self.ttl = ttl
        self._cache = OrderedDict()  # LRU cache
        self._timestamps = {}  # Entry timestamps
        self._lock = RLock()  # Thread safety
        self._hits = 0
        self._misses = 0
        
    def get(self, key: str) -> Optional[Any]:
        """
        Get result from cache if available and not expired.
        
        Args:
            key: Cache key (normalized query hash)
            
        Returns:
            Cached result or None if not found/expired
        """
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
                
            # Check if entry has expired
            entry_time = self._timestamps.get(key, 0)
            if self.ttl > 0 and time.time() - entry_time > self.ttl:
                self._remove(key)
                self._misses += 1
                return None
                
            # Move to end of LRU (most recently used)
            value = self._cache.pop(key)
            self._cache[key] = value
            self._hits += 1
            return value
            
    def set(self, key: str, value: Any) -> None:
        """
        Add or update cache entry.
        
        Args:
            key: Cache key (normalized query hash)
            value: Result to cache
        """
        with self._lock:
            # If key exists, update its position
            if key in self._cache:
                self._cache.pop(key)
                
            # Add new entry
            self._cache[key] = value
            self._timestamps[key] = time.time()
            
            # Ensure size limit
            if len(self._cache) > self.max_size:
                # Remove oldest entry (first in OrderedDict)
                oldest_key = next(iter(self._cache))
                self._remove(oldest_key)
                
    def _remove(self, key: str) -> None:
        """
        Remove entry from cache.
        
        Args:
            key: Cache key to remove
        """
        if key in self._cache:
            del self._cache[key]
        if key in self._timestamps:
            del self._timestamps[key]
            
    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()
            
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        with self._lock:
            total_requests = self._hits + self._misses
            hit_rate = self._hits / total_requests if total_requests > 0 else 0
            
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "ttl": self.ttl,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "total_requests": total_requests
            }


class QueryOptimizer:
    """
    Query optimizer for Neo4j queries.
    
    This class provides methods for optimizing Neo4j queries, including
    caching, performance monitoring, and hint management.
    """
    
    def __init__(self, cache_size: int = 100, cache_ttl: int = 3600):
        """
        Initialize query optimizer.
        
        Args:
            cache_size: Maximum number of cached queries
            cache_ttl: Time-to-live in seconds for cache entries
        """
        self.cache = QueryCache(max_size=cache_size, ttl=cache_ttl)
        self.query_metrics = {}  # Track query execution times
        
    def normalize_query(self, query: str) -> str:
        """
        Normalize Cypher query for consistent hashing.
        
        Args:
            query: Cypher query to normalize
            
        Returns:
            Normalized query string
        """
        # Remove whitespace variations
        query = re.sub(r'\s+', ' ', query.strip())
        
        # Normalize case for keywords
        pattern = re.compile(r'\b(MATCH|WHERE|RETURN|ORDER BY|SKIP|LIMIT|CREATE|SET|MERGE|DELETE|REMOVE|WITH|UNWIND)\b', re.IGNORECASE)
        query = pattern.sub(lambda m: m.group(0).upper(), query)
        
        # Remove comments
        query = re.sub(r'//.*?$', '', query, flags=re.MULTILINE)
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
        
        return query
        
    def compute_query_hash(self, query: str, parameters: Dict[str, Any]) -> str:
        """
        Compute hash for query and parameters.
        
        Args:
            query: Cypher query
            parameters: Query parameters
            
        Returns:
            Query hash string
        """
        normalized_query = self.normalize_query(query)
        
        # Convert parameters to a stable string representation
        param_str = str(sorted(parameters.items()))
        
        # Combine and hash
        return f"{hash(normalized_query)}:{hash(param_str)}"
        
    def apply_hints(self, query: str, hints: List[str]) -> str:
        """
        Apply Neo4j query hints.
        
        Args:
            query: Cypher query
            hints: List of hints to apply
            
        Returns:
            Query with hints applied
        """
        if not hints:
            return query
            
        hint_str = " ".join(f"USING {hint}" for hint in hints)
        
        # Find appropriate location to insert hints
        match = re.search(r'\b(MATCH|OPTIONAL\s+MATCH)\b', query, re.IGNORECASE)
        if match:
            # Insert hint after MATCH keyword
            pos = match.end()
            return query[:pos] + f" {hint_str} " + query[pos:]
        else:
            # Fall back to prepending hint
            return f"/*+ {' '.join(hints)} */ {query}"
            
    def optimize_query(
        self, 
        query: str, 
        parameters: Dict[str, Any], 
        hints: Optional[List[str]] = None,
        force_bypass_cache: bool = False
    ) -> Tuple[str, Dict[str, Any], Optional[str]]:
        """
        Optimize a Cypher query for execution.
        
        Args:
            query: Cypher query to optimize
            parameters: Query parameters
            hints: Optional query hints to apply
            force_bypass_cache: Whether to bypass cache lookup
            
        Returns:
            Tuple of (optimized_query, parameters, cache_key)
        """
        # Apply hints if provided
        optimized_query = self.apply_hints(query, hints or [])
        
        # Compute cache key
        cache_key = self.compute_query_hash(query, parameters)
        
        # For monitoring, even if we bypass cache
        self.query_metrics.setdefault(cache_key, {
            "count": 0,
            "total_time": 0,
            "avg_time": 0,
            "min_time": float('inf'),
            "max_time": 0,
            "last_parameters": None
        })
        
        if force_bypass_cache:
            return optimized_query, parameters, None
        
        return optimized_query, parameters, cache_key
        
    def execute_query(
        self,
        session_run_func,
        query: str,
        parameters: Dict[str, Any],
        hints: Optional[List[str]] = None,
        bypass_cache: bool = False,
        max_result_size: Optional[int] = None
    ) -> Any:
        """
        Execute a query with optimization and caching.
        
        Args:
            session_run_func: Function to run Neo4j session query
            query: Cypher query to execute
            parameters: Query parameters
            hints: Optional query hints to apply
            bypass_cache: Whether to bypass cache
            max_result_size: Maximum result size to cache
            
        Returns:
            Query results
        """
        # Optimize query
        optimized_query, parameters, cache_key = self.optimize_query(
            query, parameters, hints, bypass_cache
        )
        
        # Check cache if not bypassing
        if not bypass_cache and cache_key:
            cached_result = self.cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for query: {optimized_query[:100]}...")
                return cached_result
                
        # Execute query and measure performance
        start_time = time.time()
        try:
            result = session_run_func(optimized_query, parameters=parameters)
            
            # Convert result to cacheable form (list of dictionaries)
            if hasattr(result, 'data'):
                records = result.data()
            else:
                # Already in data form
                records = result
                
            execution_time = time.time() - start_time
            
            # Update metrics
            if cache_key:
                metrics = self.query_metrics[cache_key]
                metrics["count"] += 1
                metrics["total_time"] += execution_time
                metrics["avg_time"] = metrics["total_time"] / metrics["count"]
                metrics["min_time"] = min(metrics["min_time"], execution_time)
                metrics["max_time"] = max(metrics["max_time"], execution_time)
                metrics["last_parameters"] = {k: str(v)[:100] for k, v in parameters.items()}
                
            # Log slow queries
            if execution_time > 1.0:  # Threshold for slow query
                logger.warning(f"Slow query ({execution_time:.2f}s): {optimized_query[:100]}...")
                
            # Cache result if not bypassing and not too large
            if not bypass_cache and cache_key:
                # Check result size if limit specified
                if max_result_size is None or (
                    isinstance(records, list) and len(records) <= max_result_size
                ):
                    self.cache.set(cache_key, records)
                else:
                    logger.debug(f"Result too large to cache ({len(records)} items)")
                    
            return records
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Query error ({execution_time:.2f}s): {str(e)}")
            raise
            
    def get_query_metrics(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get metrics for executed queries.
        
        Args:
            limit: Maximum number of queries to return, sorted by average time
            
        Returns:
            List of query metrics
        """
        # Sort by average execution time (descending)
        sorted_metrics = sorted(
            [
                {"query_hash": k, **v} 
                for k, v in self.query_metrics.items()
                if v["count"] > 0  # Only include executed queries
            ],
            key=lambda x: x["avg_time"],
            reverse=True
        )
        
        return sorted_metrics[:limit]
        
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        return self.cache.get_stats()
        
    def clear_cache(self) -> None:
        """Clear query cache."""
        self.cache.clear()
        logger.info("Query cache cleared")
        
    def reset_metrics(self) -> None:
        """Reset query performance metrics."""
        self.query_metrics = {}
        logger.info("Query metrics reset")
