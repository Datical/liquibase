package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.database.core.OracleDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.AbstractSqlGeneratorTest;
import liquibase.statement.core.AlterSequenceStatement;
import liquibase.test.TestContext;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AlterSequenceGeneratorTest extends AbstractSqlGeneratorTest<AlterSequenceStatement> {
	
	protected static final String SEQUENCE_NAME = "SEQUENCE_NAME";
    protected static final String CATALOG_NAME = "CATALOG_NAME";
    protected static final String SCHEMA_NAME = "SCHEMA_NAME";
	
    public AlterSequenceGeneratorTest() throws Exception {
        setUnderTest(new AlterSequenceGenerator());
    }

	@Test
    public void testAlterSequenceDatabase() throws Exception {
    	for (Database database : TestContext.getInstance().getAllDatabases()) {
    		if (database instanceof OracleDatabase) {
    			AlterSequenceStatement statement = new AlterSequenceStatement(CATALOG_NAME, SCHEMA_NAME, SEQUENCE_NAME);
	    		statement.setCacheSize(BigInteger.valueOf(3000L));

	    		Sql[] generatedSql = this.getGeneratorUnderTest().generateSql(statement, database, null);

    			assertEquals("ALTER SEQUENCE CATALOG_NAME.SEQUENCE_NAME CACHE 3000", generatedSql[0].toSql());
    		}
    	}
    }

	@Override
	protected AlterSequenceStatement createSampleSqlStatement() {
		AlterSequenceStatement statement = new AlterSequenceStatement(CATALOG_NAME, SCHEMA_NAME, SEQUENCE_NAME);
        return statement;
	}
	
	@Override
	protected boolean shouldBeImplementation(Database database) {
		return database.supportsSequences();
	}
}
