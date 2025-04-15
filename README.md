# Knowledge Storage MCP

Model Context Protocol server for the Knowledge Extraction System that manages storage, retrieval, and querying of academic knowledge with support for symbol-concept separation.

## Overview

The Knowledge Storage MCP serves as the central repository for structured academic knowledge extracted from scientific papers. It provides a robust, scalable infrastructure for storing, retrieving, and querying knowledge entities, relationships, and their associated metadata.

## Core Functionality

- Entity and relationship storage with comprehensive metadata
- Schema validation and enforcement for academic content
- Efficient query capabilities with specialized patterns for academic knowledge
- Versioning and provenance tracking
- Cross-referencing between knowledge entities

## Architectural Principles

### Symbol-Concept Separation
This MCP explicitly models symbols and concepts as separate entity types with relationships between them, supporting the system's symbol-concept separation principle.

### Tiered Knowledge Organization
Implements tiered knowledge storage (L1, L2, L3) through property structures and specialized query patterns, allowing efficient knowledge retrieval at appropriate detail levels.

### Cross-Domain Mapping
Supports rich relationship types between entities, enabling explicit mapping between concepts across different domains, facilitating cross-domain knowledge integration.

## Installation

```bash
# Clone the repository
git clone https://github.com/AbbyFraenkel/knowledge-storage-mcp.git
cd knowledge-storage-mcp

# Setup virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Development

```bash
# Start Neo4j container
docker-compose up -d neo4j

# Run the MCP server
python -m knowledge_storage_mcp.server
```

## Documentation

Detailed API documentation is available in the `/docs` directory.
