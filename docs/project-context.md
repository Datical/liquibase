# Liquibase Project Context

## Business Context

Liquibase is an open-source database schema change management solution. It enables teams to track, version, and deploy database changes across different environments in a controlled and repeatable manner.

**Key Value Propositions:**
- Database-agnostic schema migration tool
- Tracks all database changes in version control
- Supports rollback capabilities
- Integrates with CI/CD pipelines
- Reduces deployment risks through controlled migrations

## Domain Knowledge

### Core Concepts

- **ChangeSet**: The atomic unit of change in Liquibase. Each changeset has a unique ID and author combination.
- **ChangeLog**: A file (XML, YAML, JSON, or SQL) that contains an ordered list of changesets.
- **DATABASECHANGELOG**: A tracking table that records which changesets have been executed.
- **DATABASECHANGELOGLOCK**: A lock table that prevents concurrent migrations.
- **Preconditions**: Conditions that must be met before a changeset executes.
- **Contexts**: Labels that allow conditional execution of changesets based on environment.
- **Labels**: Another filtering mechanism for changeset execution.

### Supported Database Operations

- DDL: CREATE, ALTER, DROP for tables, views, indexes, sequences
- DML: INSERT, UPDATE, DELETE data
- Stored procedures and functions
- Custom SQL execution
- Database-specific operations

## Project Version

- Current Version: 3.5.2-SNAPSHOT
- Group ID: org.liquibase
- License: Apache License 2.0

## Key Features

1. **Multi-format Changelog Support**: XML, YAML, JSON, SQL
2. **Database Abstraction**: Single changelog works across multiple database platforms
3. **Rollback Support**: Automatic and custom rollback generation
4. **Diff Generation**: Compare databases and generate migration scripts
5. **DBDoc Generation**: Generate database documentation
6. **Maven Plugin Integration**: Run migrations from Maven builds
7. **Ant Task Integration**: Run migrations from Ant builds
8. **Spring Integration**: Integrate with Spring applications
9. **Servlet Listener**: Auto-run migrations on web application startup
10. **OSGi Bundle**: Deploy in OSGi containers

## Integration Points

### Build Tool Integration
- Maven Plugin (liquibase-maven-plugin)
- Ant Tasks (liquibase-core integration.ant package)

### Framework Integration
- Spring Framework (SpringLiquibase)
- Servlet Container (LiquibaseServletListener)
- OSGi (Bundle manifest in liquibase-core)

### Database Support
The core supports multiple databases through the `liquibase.database` package. Each database has a specific implementation that handles dialect differences.

## Constraints

- **Java Version**: Requires Java 21+
- **Maven Version**: Requires Maven 3.8.4+
- **Database Compatibility**: Must maintain backwards compatibility with existing changelog formats
- **Thread Safety**: Lock mechanism ensures single-node execution

## Design Decisions

1. **Service Locator Pattern**: Uses `ServiceLocator` for plugin discovery and extensibility
2. **Changelog Parsing**: Supports multiple formats through parser abstraction
3. **Database Abstraction**: All database operations go through `Database` interface
4. **Statement/Generator Pattern**: SQL generation separated from execution
5. **Snapshot Mechanism**: Captures database state for comparison and diff operations

---

## Module-Specific Context

### liquibase-core Module

**Purpose**: Core engine containing all database migration functionality.

**Key Packages:**
- `liquibase.change`: Change types (AddColumn, CreateTable, etc.)
- `liquibase.changelog`: Changelog parsing and management
- `liquibase.database`: Database abstraction layer
- `liquibase.diff`: Database comparison and diff generation
- `liquibase.executor`: SQL execution layer
- `liquibase.lockservice`: Distributed lock management
- `liquibase.parser`: Changelog file parsing
- `liquibase.precondition`: Pre-execution condition checks
- `liquibase.snapshot`: Database state capture
- `liquibase.sql`: SQL statement types
- `liquibase.sqlgenerator`: Database-specific SQL generation

**Dependencies:**
- commons-cli:1.9.0 (optional, for CLI)
- snakeyaml:2.4 (YAML changelog support)
- ant:1.10.15 (provided, for Ant integration)
- osgi.core:8.0.0 (provided, for OSGi support)
- servlet-api:2.4 (provided, for servlet integration)
- spring:2.0.6 (provided, for Spring integration)

**Build Artifacts:**
- Main JAR: liquibase-{version}.jar
- Test JAR: liquibase-{version}-tests.jar
- OSGi Bundle (with manifest)
- Distribution archive (bin.xml assembly)

### liquibase-maven-plugin Module

**Purpose**: Maven plugin that wraps Liquibase functionality for Maven builds.

**Key Features:**
- Goal-based execution (update, rollback, generateChangeLog, etc.)
- Project classpath integration
- Maven property interpolation

**Dependencies:**
- maven-plugin-api:3.9.10
- maven-core:3.9.10
- maven-compat:3.9.10
- liquibase-core

**Usage:**
```xml
<plugin>
    <groupId>org.liquibase</groupId>
    <artifactId>liquibase-maven-plugin</artifactId>
    <version>3.5.2-SNAPSHOT</version>
    <configuration>
        <changeLogFile>src/main/resources/db/changelog.xml</changeLogFile>
        <url>jdbc:postgresql://localhost/mydb</url>
    </configuration>
</plugin>
```

### liquibase-integration-tests Module

**Purpose**: Integration tests that verify Liquibase functionality against real databases.

**Key Features:**
- Tests against multiple databases (H2, HSQLDB, Derby)
- Maven Verifier tests for plugin verification
- Ant integration tests
- Spock-based tests for behavior verification

**Test Dependencies:**
- junit-jupiter:5.13.3
- spock-core:2.4-M6-groovy-4.0
- hsqldb:2.7.4
- derby:10.17.1.0
- maven-verifier:2.0.0-M1

**Profiles:**
- `oracle`: Adds Oracle JDBC driver (ojdbc8:18.3.0.0) for Oracle testing

**Note**: This module has `packaging: pom` since it only contains tests, no main source code.

### liquibase-debian Module

**Purpose**: Builds Debian (.deb) package for Linux distribution.

**Activation**: Only activated on Linux systems (`<os><family>linux</family></os>`).

### liquibase-rpm Module

**Purpose**: Builds RPM package for Red Hat/CentOS distribution.

**Activation**: Only activated on Linux systems (`<os><family>linux</family></os>`).
