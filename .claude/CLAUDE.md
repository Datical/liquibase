# Liquibase - Claude Code Instructions

## Tech Stack

- **Language**: Java 21
- **Build**: Maven 3.8.4+ (multi-module project)
- **Testing**: JUnit Jupiter 5.13.3, Spock 2.4-M6-groovy-4.0, Mockito 5.18.0
- **Logging**: Custom logging framework (liquibase.logging)
- **Serialization**: SnakeYAML 2.4 for YAML support
- **Code Quality**: JaCoCo for coverage, SonarCloud for analysis

## Project Structure

```
liquibase/
+-- pom.xml (parent POM)
+-- liquibase-core/ (core engine)
+-- liquibase-maven-plugin/ (Maven integration)
+-- liquibase-integration-tests/ (integration tests)
+-- liquibase-debian/ (Linux deb packaging)
+-- liquibase-rpm/ (Linux rpm packaging)
```

## Build Commands

```bash
# Full build
mvn clean install

# Skip tests
mvn clean install -DskipTests

# Run tests only
mvn test

# Build specific module
mvn clean install -pl liquibase-core

# Build with dependencies
mvn clean install -pl liquibase-maven-plugin -am

# Run integration tests
mvn verify -pl liquibase-integration-tests

# Run with Oracle profile
mvn verify -Poracle
```

## Code Patterns

### Adding a New Change Type

1. Create class in `liquibase-core/src/main/java/liquibase/change/core/`
2. Extend `AbstractChange`
3. Annotate with `@DatabaseChange`
4. Implement `generateStatements()` and `createInverses()`
5. Register in `META-INF/services/liquibase.change.Change`

### Adding Database Support

1. Create class in `liquibase-core/src/main/java/liquibase/database/core/`
2. Extend `AbstractJdbcDatabase`
3. Override database-specific methods
4. Register in `META-INF/services/liquibase.database.Database`

### Adding SQL Generator

1. Create class in `liquibase-core/src/main/java/liquibase/sqlgenerator/core/`
2. Extend `AbstractSqlGenerator<StatementType>`
3. Set priority for database-specific generators
4. Register in `META-INF/services/liquibase.sqlgenerator.SqlGenerator`

## Anti-Patterns

**AVOID:**
- Direct SQL string construction without escaping (use `database.escapeObjectName()`)
- Hardcoding database-specific syntax in Change classes (use SqlGenerator)
- Breaking backwards compatibility in changelog parsing
- Modifying DATABASECHANGELOG table structure without migration path
- Ignoring transaction boundaries in multi-statement changes

**DO NOT:**
- Add new dependencies without updating parent POM dependencyManagement
- Skip precondition validation in change execution
- Bypass LockService for database modifications
- Create new changelog formats without parser registration

## Quality Gates

### Before Commit

1. Run `mvn verify` to ensure all tests pass
2. Check for new compiler warnings
3. Ensure changelog formats remain backwards compatible
4. Verify precondition checks for new changes

### Code Review Focus

- Database abstraction maintained
- SQL injection prevention
- Rollback support implemented
- Thread safety considerations
- Backwards compatibility

## Testing Strategy

### Unit Tests

- Location: `src/test/java/` in each module
- Framework: JUnit Jupiter
- Mock external dependencies with Mockito
- Test SQL generation with MockDatabase

### Integration Tests

- Location: `liquibase-integration-tests/`
- Framework: Spock + JUnit
- Tests run against H2, HSQLDB, Derby (embedded)
- Use `@Disabled` for database-specific tests requiring external setup

### Running Specific Tests

```bash
# Single test class
mvn test -Dtest=CreateTableChangeTest

# Single test method
mvn test -Dtest=CreateTableChangeTest#testGenerateStatements

# Pattern matching
mvn test -Dtest=*OracleTest
```

## File Organization

### Source Files

```
src/main/java/liquibase/
+-- change/           # Change type definitions
+-- changelog/        # Changelog parsing and processing
+-- database/         # Database abstractions
+-- diff/             # Database comparison
+-- executor/         # SQL execution
+-- parser/           # Changelog file parsers
+-- precondition/     # Precondition checks
+-- snapshot/         # Database state capture
+-- sql/              # SQL statements
+-- sqlgenerator/     # SQL generation
```

### Resource Files

```
src/main/resources/
+-- META-INF/
    +-- services/     # ServiceLoader registrations
    +-- MANIFEST.MF   # OSGi bundle manifest
+-- liquibase.build.properties
```

## Naming Conventions

### Classes

- Changes: `{Action}{Object}Change` (e.g., `CreateTableChange`, `AddColumnChange`)
- Databases: `{DatabaseName}Database` (e.g., `OracleDatabase`, `PostgresDatabase`)
- Generators: `{Statement}Generator{Database}` (e.g., `CreateTableGeneratorOracle`)
- Statements: `{Action}{Object}Statement` (e.g., `CreateTableStatement`)

### Packages

- Database-specific: `liquibase.*.core` (e.g., `liquibase.database.core`)
- Integration: `liquibase.integration.*` (e.g., `liquibase.integration.ant`)

### Test Classes

- Unit tests: `{ClassName}Test`
- Spock specs: `{ClassName}Spec`
- Integration tests: `{Feature}IntegrationTest`

## IDE Setup

### IntelliJ IDEA

- Import as Maven project
- Enable Groovy plugin for Spock tests
- Set Java SDK to 21
- Configure Groovy SDK from Maven dependency

### Useful Run Configurations

- `mvn clean install -DskipTests` - Fast build
- `mvn test -pl liquibase-core` - Core tests only
- `mvn verify` - Full verification including integration tests
