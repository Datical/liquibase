package liquibase.sqlgenerator.core;

import liquibase.sqlgenerator.AbstractSqlGeneratorTest;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

public class UnlockDatabaseChangeLogGeneratorTest extends AbstractSqlGeneratorTest<UnlockDatabaseChangeLogStatement> {

    public UnlockDatabaseChangeLogGeneratorTest() throws Exception {
        setUnderTest(new UnlockDatabaseChangeLogGenerator());
    }

    @Override
    protected UnlockDatabaseChangeLogStatement createSampleSqlStatement() {
        return new UnlockDatabaseChangeLogStatement();
    }
}
