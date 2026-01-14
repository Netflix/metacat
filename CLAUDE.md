# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Full build with tests
./gradlew clean build

# Build without tests
./gradlew assemble

# Run unit tests for all modules
./gradlew test

# Run tests for a specific module
./gradlew :metacat-connector-polaris:test

# Run a single test class (Spock)
./gradlew :metacat-main:test --tests "TableServiceImplSpec"

# Run checkstyle
./gradlew checkstyleMain checkstyleTest

# Run SpotBugs
./gradlew spotbugsMain
```

## Functional Tests

Functional tests require Docker and spin up test clusters:

```bash
# Run functional tests with CRDB backend
./gradlew :metacat-functional-tests:functionalTestCRDB

# Run functional tests with Aurora backend
./gradlew :metacat-functional-tests:functionalTestAurora

# Run Polaris connector functional tests
./gradlew :metacat-connector-polaris:functionalTestCRDB
./gradlew :metacat-connector-polaris:functionalTestAurora

# Start/stop test cluster manually
./gradlew metacatPorts        # Start and print port mappings
./gradlew stopMetacatCluster  # Stop cluster
```

## Architecture Overview

Metacat is a unified metadata exploration API that federates views across multiple data systems (Polaris, Cassandra, MySQL, PostgreSQL, Redshift, S3, Druid, Snowflake).

### Module Structure

**Core Modules:**
- `metacat-main` - Spring Boot application with REST controllers, services, and orchestration
- `metacat-common` - Shared DTOs, models, and exceptions
- `metacat-common-server` - Server-side connector interfaces, converters, and user metadata services
- `metacat-war` - WAR packaging for deployment

**Connectors** (plugin pattern):
- `metacat-connector-polaris` - Apache Iceberg Polaris metadata
- `metacat-connector-{mysql,postgresql,redshift,snowflake,cassandra,druid,s3,pig,jdbc}` - Data system connectors

**Supporting:**
- `metacat-metadata`, `metacat-metadata-mysql` - User metadata persistence
- `metacat-thrift` - Thrift interface for RPC
- `metacat-client` - Client library for Metacat APIs

### Connector Plugin Architecture

Each connector implements `ConnectorPlugin` and provides:
- `ConnectorFactory` - Creates connector service instances
- `ConnectorDatabaseService`, `ConnectorTableService`, `ConnectorPartitionService` - CRUD operations
- `ConnectorTypeConverter` - Type mapping between Metacat and the underlying system
- `ConnectorExceptionMapper` - Exception translation

Connectors are discovered via `ConnectorManager` and loaded from catalog `.properties` files.

### Service Layer Pattern

```
REST Controller → Service (e.g., TableServiceImpl) → ConnectorManager → Connector Service
                      ↓
              Pre/Post Events (for ES indexing, SNS notifications)
                      ↓
              UserMetadataService (custom metadata persistence)
```

### Key Classes

- `QualifiedName` - Represents `catalog/database/table` hierarchy (used throughout)
- `MetacatContextManager` - Request context tracking via ThreadLocal
- `ConnectorManager` - Registry of active connectors
- `DefaultCatalogManager` - Loads catalog configurations

## Testing Conventions

- **Unit tests**: Spock framework (Groovy), located in `src/test/groovy/`, named `*Spec.groovy`
- **Functional tests**: JUnit, located in `src/functionalTest/`, require Docker containers
- Tests use given/when/then BDD style with Spock

## Configuration

- Java 17 required
- Spring Boot 3.2.x
- Catalog configs loaded from `metacat.plugin.config.location` directory
- User metadata config at `metacat.usermetadata.config.location`
