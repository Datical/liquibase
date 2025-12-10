# Liquibase Code Patterns

## Component Patterns

### Service Locator Pattern

Liquibase uses a service locator for plugin discovery and extensibility:

```java
// Finding all implementations of an interface
ServiceLocator locator = ServiceLocator.getInstance();
List<Database> databases = locator.findInstances(Database.class);

// Services register via META-INF/services files
// e.g., META-INF/services/liquibase.database.Database
```

### Factory Pattern

SQL generation uses factory pattern for database-specific implementations:

```java
// SqlGeneratorFactory selects appropriate generator
SqlGeneratorFactory factory = SqlGeneratorFactory.getInstance();
Sql[] sql = factory.generateSql(statement, database);

// Generators are registered and selected by priority
@LiquibaseService
public class CreateTableGeneratorOracle extends AbstractSqlGenerator<CreateTableStatement> {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;  // Higher priority for database-specific
    }
}
```

### Visitor Pattern

Changelog processing uses visitor pattern:

```java
// ChangeSetVisitor interface
public interface ChangeSetVisitor {
    Direction getDirection();
    void visit(ChangeSet changeSet, DatabaseChangeLog changeLog, Database database);
}

// Used by Liquibase for various operations
changeLog.accept(new ChangeSetVisitor() {
    @Override
    public void visit(ChangeSet changeSet, DatabaseChangeLog changeLog, Database database) {
        // Process each changeset
    }
});
```

### Template Method Pattern

Abstract base classes define algorithms with extension points:

```java
// AbstractChange defines the change processing template
public abstract class AbstractChange implements Change {
    // Template method
    public final SqlStatement[] generateStatements(Database database) {
        // Pre-processing
        validate(database);
        // Delegate to subclass
        return generateStatementsVolatile(database);
    }

    // Override point
    protected abstract SqlStatement[] generateStatementsVolatile(Database database);
}
```

## State Management Patterns

### Lock Service Pattern

Distributed lock management for concurrent access:

```java
// Acquire lock before migrations
LockService lockService = LockServiceFactory.getInstance().getLockService(database);
lockService.waitForLock();

try {
    // Perform migrations
} finally {
    lockService.releaseLock();
}
```

### Change History Tracking

All changes tracked in DATABASECHANGELOG:

```java
// Check if changeset was already executed
ChangeLogHistoryService historyService = ChangeLogHistoryServiceFactory.getInstance()
    .getChangeLogService(database);

List<RanChangeSet> ranChangeSets = historyService.getRanChangeSets();

// Record execution after successful run
historyService.setExecType(changeSet, execType);
```

## API Integration Patterns

### Database Abstraction

All database operations go through the Database interface:

```java
// Database interface abstracts dialect differences
public interface Database {
    String getShortName();
    String getDefaultDriver(String url);
    boolean supportsSequences();
    String escapeObjectName(String objectName, Class<? extends DatabaseObject> objectType);
    // ...
}

// Implementations handle database-specific behavior
public class PostgresDatabase extends AbstractJdbcDatabase {
    @Override
    public String getShortName() {
        return "postgresql";
    }
}
```

### Statement/Generator Separation

SQL statement structure separated from generation:

```java
// Statement defines what to do (data)
public class CreateTableStatement extends AbstractSqlStatement {
    private String tableName;
    private List<ColumnConfig> columns;
    // ...
}

// Generator defines how to do it (behavior)
public class CreateTableGenerator extends AbstractSqlGenerator<CreateTableStatement> {
    @Override
    public Sql[] generateSql(CreateTableStatement statement, Database database, ...) {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(database.escapeTableName(...));
        // Database-specific SQL generation
        return new Sql[] { new UnparsedSql(sql.toString()) };
    }
}
```

## Error Handling Patterns

### Custom Exception Hierarchy

```java
// Base exception
public class LiquibaseException extends Exception {
    // Common exception handling
}

// Specific exceptions
public class DatabaseException extends LiquibaseException {
    // Database-related errors
}

public class ChangeLogParseException extends LiquibaseException {
    // Parsing errors
}

public class PreconditionFailedException extends LiquibaseException {
    // Precondition check failures
}
```

### Precondition Validation

Conditional execution with clear failure modes:

```java
// Precondition check before changeset execution
public interface Precondition {
    void check(Database database, DatabaseChangeLog changeLog, ChangeSet changeSet)
        throws PreconditionFailedException, PreconditionErrorException;
}

// Usage in changeset
<changeSet id="1" author="dev">
    <preConditions onFail="MARK_RAN">
        <tableExists tableName="existing_table"/>
    </preConditions>
    <!-- changes -->
</changeSet>
```

## Testing Patterns

### JUnit Jupiter Tests

```java
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class CreateTableChangeTest {
    private CreateTableChange change;

    @BeforeEach
    void setUp() {
        change = new CreateTableChange();
    }

    @Test
    void generateStatements_createsValidSQL() {
        change.setTableName("test_table");
        // Add columns...

        SqlStatement[] statements = change.generateStatements(new MockDatabase());

        assertNotNull(statements);
        assertEquals(1, statements.length);
    }
}
```

### Spock Specifications (Groovy)

```groovy
import spock.lang.Specification

class DatabaseFactorySpec extends Specification {

    def "findCorrectDatabaseImplementation returns correct database for URL"() {
        given:
        def factory = DatabaseFactory.getInstance()

        when:
        def database = factory.findCorrectDatabaseImplementation(url)

        then:
        database.class == expectedClass

        where:
        url                                    | expectedClass
        "jdbc:postgresql://localhost/test"    | PostgresDatabase
        "jdbc:mysql://localhost/test"         | MySQLDatabase
        "jdbc:h2:mem:test"                    | H2Database
    }
}
```

### Mockito Mocking

```java
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ExecutorTest {

    @Mock
    private Database database;

    @Mock
    private DatabaseConnection connection;

    @Test
    void execute_callsConnectionExecute() {
        when(database.getConnection()).thenReturn(connection);

        executor.execute(statement, database);

        verify(connection).execute(any(String.class));
    }
}
```

---

## Module-Specific Patterns

### liquibase-core Patterns

**Change Implementation Pattern:**
```java
@DatabaseChange(
    name = "createTable",
    description = "Creates a new table",
    priority = ChangeMetaData.PRIORITY_DEFAULT
)
public class CreateTableChange extends AbstractChange {

    private String tableName;
    private String schemaName;
    private List<ColumnConfig> columns;

    // Getters/setters with @DatabaseChangeProperty annotations
    @DatabaseChangeProperty(description = "Name of the table to create")
    public String getTableName() {
        return tableName;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        CreateTableStatement statement = new CreateTableStatement(
            getCatalogName(), getSchemaName(), getTableName());
        // Add columns to statement
        return new SqlStatement[] { statement };
    }

    @Override
    protected Change[] createInverses() {
        DropTableChange inverse = new DropTableChange();
        inverse.setTableName(getTableName());
        return new Change[] { inverse };
    }
}
```

**Database Implementation Pattern:**
```java
public class OracleDatabase extends AbstractJdbcDatabase {

    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) {
        return getDatabaseProductName(conn).startsWith("Oracle");
    }

    @Override
    public String getShortName() {
        return "oracle";
    }

    @Override
    public boolean supportsSequences() {
        return true;
    }

    @Override
    public String getAutoIncrementClause() {
        // Oracle uses sequences, not auto-increment
        return "";
    }
}
```

### liquibase-maven-plugin Patterns

**Maven Goal Pattern:**
```java
@Mojo(
    name = "update",
    defaultPhase = LifecyclePhase.PROCESS_RESOURCES
)
public class LiquibaseUpdate extends AbstractLiquibaseMojo {

    @Parameter(property = "liquibase.changeLogFile", required = true)
    protected String changeLogFile;

    @Parameter(property = "liquibase.contexts")
    protected String contexts;

    @Override
    protected void performLiquibaseTask(Liquibase liquibase) throws LiquibaseException {
        liquibase.update(new Contexts(contexts), new LabelExpression(labels));
    }
}
```

### liquibase-integration-tests Patterns

**Integration Test Pattern:**
```java
public abstract class AbstractIntegrationTest {

    protected Database database;
    protected Liquibase liquibase;

    @BeforeEach
    void setUp() throws Exception {
        database = DatabaseFactory.getInstance()
            .findCorrectDatabaseImplementation(new JdbcConnection(getConnection()));
        liquibase = new Liquibase(getChangeLogFile(),
            new ClassLoaderResourceAccessor(), database);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (database != null) {
            database.close();
        }
    }

    protected abstract Connection getConnection() throws SQLException;
    protected abstract String getChangeLogFile();
}
```

**Maven Verifier Pattern:**
```java
public class MavenPluginIntegrationTest {

    @Test
    void updateGoal_executesSuccessfully() throws Exception {
        File testDir = ResourceExtractor.simpleExtractResources(
            getClass(), "/integration-test-project");

        Verifier verifier = new Verifier(testDir.getAbsolutePath());
        verifier.executeGoal("liquibase:update");
        verifier.verifyErrorFreeLog();

        // Verify database state
        verifier.verifyTextInLog("Successfully acquired change log lock");
    }
}
```
