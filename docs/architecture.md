# Liquibase Architecture

## High-Level Architecture

```
+-- User Interface Layer
    +-- CLI (Main class in liquibase.integration.commandline)
    +-- Maven Plugin (liquibase-maven-plugin)
    +-- Ant Tasks (liquibase.integration.ant)
    +-- Servlet Listener (liquibase.integration.servlet)
    +-- Spring Integration (liquibase.integration.spring)

+-- Core API Layer (liquibase.Liquibase)
    +-- Changelog Parsing
    +-- Change Execution
    +-- Rollback Management
    +-- Diff/Snapshot Operations

+-- Database Abstraction Layer
    +-- Database Interface
    +-- SQL Generator Layer
    +-- Executor Layer

+-- Database Drivers (JDBC)
    +-- PostgreSQL, MySQL, Oracle, SQL Server, etc.
```

## Component Relationships

### Entry Points

All interactions flow through the main `Liquibase` class which orchestrates:
1. Changelog parsing
2. Database connection management
3. Lock acquisition
4. Change execution
5. History tracking

### Core Flow: Update Operation

```
1. Liquibase.update() called
       |
       v
2. LockService.acquireLock()
       |
       v
3. ChangeLogParser.parse(changelogFile)
       |
       v
4. For each ChangeSet:
       +-- Check preconditions
       +-- Check if already ran (DATABASECHANGELOG)
       +-- Generate SQL via SqlGenerator
       +-- Execute via Executor
       +-- Record in DATABASECHANGELOG
       |
       v
5. LockService.releaseLock()
```

## Data Flow

### Changelog Processing

```
Changelog File (XML/YAML/JSON/SQL)
       |
       v
ChangeLogParser (selects appropriate parser)
       |
       v
DatabaseChangeLog (parsed object model)
       |
       v
ChangeSet objects (executable units)
       |
       v
Change objects (AddColumn, CreateTable, etc.)
       |
       v
Statement objects (SqlStatement implementations)
       |
       v
SqlGenerator (database-specific SQL)
       |
       v
Executor (JDBC execution)
```

### SQL Generation Pipeline

```
Change.generateStatements(Database)
       |
       v
SqlStatement (e.g., CreateTableStatement)
       |
       v
SqlGeneratorFactory.generateSql(statement, database)
       |
       v
SqlGenerator.generateSql(statement, database, sqlGeneratorChain)
       |
       v
Sql[] (executable SQL strings)
```

## Deployment Architecture

### Standalone Deployment

```
+-- Application Server / JVM
    +-- liquibase-core.jar
    +-- Database JDBC Driver
    +-- changelog files (classpath or filesystem)
```

### Maven Build Integration

```
+-- Maven Build Lifecycle
    +-- liquibase-maven-plugin
        +-- liquibase-core (transitive)
        +-- Project classpath (JDBC drivers, etc.)
    +-- Executes during build phase (e.g., process-resources)
```

### Web Application Deployment

```
+-- Web Application (WAR)
    +-- WEB-INF/lib/
        +-- liquibase-core.jar
        +-- JDBC driver
    +-- WEB-INF/web.xml
        +-- LiquibaseServletListener
    +-- changelog files in classpath
```

## Technology Decisions

### Language and Platform
- **Java 21**: Target runtime (configured in maven-compiler-plugin)
- **Groovy 4.0**: Used for Spock tests
- **Maven 3.8.4+**: Build system requirement

### Serialization
- **SnakeYAML 2.4**: YAML changelog support
- **Custom XML parsing**: Direct SAX/DOM parsing for XML changelogs
- **JSON support**: Built-in JSON changelog support

### Extension Mechanism
- **ServiceLoader pattern**: Plugin discovery via META-INF/services
- **Priority-based selection**: Multiple implementations sorted by priority

### OSGi Support
- **Bundle manifest**: Generated via maven-bundle-plugin
- **Import-Package**: Configured for optional dependencies

## Scalability Considerations

### Database Lock Mechanism
- Uses DATABASECHANGELOGLOCK table
- Prevents concurrent migrations
- Single-node execution model
- Lock timeout configurable

### Changelog Organization
- **Include/IncludeAll**: Split changelogs across files
- **Context/Label filtering**: Selective changeset execution
- **Preconditions**: Runtime conditional execution

### Performance
- Changelog parsing happens once per execution
- Statement caching in SqlGeneratorFactory
- Connection pooling delegated to calling application

---

## Module-Specific Architectures

### liquibase-core Architecture

**Package Structure:**
```
liquibase/
+-- Liquibase.java (Main facade class)
+-- CatalogAndSchema.java
+-- Contexts.java, Labels.java
+-- change/
    +-- core/ (Built-in changes: AddColumn, CreateTable, etc.)
    +-- custom/ (Custom change interfaces)
+-- changelog/
    +-- DatabaseChangeLog.java
    +-- ChangeSet.java
    +-- filter/ (Changeset filtering)
    +-- visitor/ (Visitor pattern for processing)
+-- command/ (Command pattern for operations)
+-- configuration/ (Configuration management)
+-- database/
    +-- Database.java (Main interface)
    +-- AbstractJdbcDatabase.java
    +-- core/ (Database implementations)
+-- diff/
    +-- DiffGenerator.java
    +-- compare/ (Object comparison)
    +-- output/ (Diff output generation)
+-- exception/ (Custom exceptions)
+-- executor/
    +-- Executor.java
    +-- jvm/JdbcExecutor.java
+-- lockservice/
    +-- LockService.java
    +-- LockServiceImpl.java
+-- parser/
    +-- core/xml/, core/yaml/, core/json/
+-- precondition/
    +-- Precondition.java
    +-- core/ (Built-in preconditions)
+-- serializer/ (Output serialization)
+-- servicelocator/ (Plugin discovery)
+-- snapshot/ (Database state capture)
+-- sql/ (SQL statement representations)
+-- sqlgenerator/ (Database-specific SQL generation)
+-- statement/ (Statement types)
+-- structure/ (Database object model)
+-- util/ (Utilities)
```

**Key Classes:**
- `Liquibase`: Main entry point, orchestrates all operations
- `Database`: Abstraction for database-specific behavior
- `ChangeSet`: Unit of database change
- `SqlGenerator`: Converts statements to database-specific SQL
- `Executor`: Executes SQL against the database

### liquibase-maven-plugin Architecture

**Goal Structure:**
```
liquibase-maven-plugin/
+-- src/main/java/org/liquibase/maven/plugins/
    +-- AbstractLiquibaseMojo.java (Base for all goals)
    +-- LiquibaseUpdate.java (update goal)
    +-- LiquibaseRollback.java (rollback goal)
    +-- LiquibaseDiff.java (diff goal)
    +-- etc.
```

**Mojo Pattern:**
- Each goal extends AbstractLiquibaseMojo
- Configuration via Maven @Parameter annotations
- Classpath handling for JDBC drivers
- Property interpolation from Maven project

### liquibase-integration-tests Architecture

**Test Organization:**
```
liquibase-integration-tests/
+-- src/test/java/
    +-- Integration test classes
+-- src/test/groovy/
    +-- Spock specifications
+-- src/test/resources/
    +-- Test changelogs
    +-- Database configurations
+-- src/test/filtered-resources/
    +-- Property-filtered resources
```

**Test Profiles:**
- Default: H2, HSQLDB, Derby (embedded databases)
- `oracle`: Oracle database testing with JDBC driver
