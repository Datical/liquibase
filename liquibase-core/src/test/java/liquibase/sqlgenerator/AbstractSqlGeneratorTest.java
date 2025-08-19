package liquibase.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.test.TestContext;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractSqlGeneratorTest<T extends SqlStatement> {

    private SqlGenerator<T> generatorUnderTest;

    public void setUnderTest(SqlGenerator<T> generatorUnderTest) throws Exception {
        this.generatorUnderTest = generatorUnderTest;
    }

    public SqlGenerator<T> getGeneratorUnderTest() {
        return this.generatorUnderTest;
    }

    protected abstract T createSampleSqlStatement();

    protected void dropAndCreateTable(CreateTableStatement statement, Database database) throws SQLException, DatabaseException {
        ExecutorService.getInstance().getExecutor(database).execute(statement);

        if (!database.getAutoCommitMode()) {
            database.getConnection().commit();
        }

    }

    @Test
    public void isImplementation() throws Exception {
        for (Database database : TestContext.getInstance().getAllDatabases()) {
            boolean isImpl = generatorUnderTest.supports(createSampleSqlStatement(), database);
            if (shouldBeImplementation(database)) {
                assertTrue(isImpl, "Unexpected false supports for " + database.getShortName());
            } else {
                assertFalse(isImpl, "Unexpected true supports for " + database.getShortName());
            }
        }
    }

    @Test
    public void isValid() throws Exception {
        for (Database database : TestContext.getInstance().getAllDatabases()) {
        	if (shouldBeImplementation(database)) {
            	if (waitForException(database)) {
            		assertTrue(generatorUnderTest.validate(createSampleSqlStatement(), database, new MockSqlGeneratorChain()).hasErrors(), "The validation should be failed for " + database);
            	} else {
            		assertFalse(generatorUnderTest.validate(createSampleSqlStatement(), database, new MockSqlGeneratorChain()).hasErrors(), "isValid failed against " + database);
            	}
            	
        	} 
        }
    }

    @Test
    public void checkExpectedGenerator() {
        assertEquals(this.getClass().getName().replaceFirst("Test$", ""), generatorUnderTest.getClass().getName());
    }

    protected boolean waitForException(Database database) {
        return false;
    }

    protected boolean shouldBeImplementation(Database database) {
        return true;
    }



}
