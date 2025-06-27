package liquibase.change;

import liquibase.change.core.AddColumnChange;
import liquibase.database.core.DB2Database;
import liquibase.sdk.database.MockDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.AddColumnStatement;
import liquibase.statement.core.ReorganizeTableStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AddColumnChangeTest {

    @Test
    public void generateStatements_multipleColumns() {
        AddColumnChange change = new AddColumnChange();
        AddColumnConfig column1 = new AddColumnConfig();
        column1.setName("column1");
        column1.setType("INT");
        change.addColumn(column1);
        AddColumnConfig column2 = new AddColumnConfig();
        column2.setName("column2");
        column2.setType("INT");
        change.addColumn(column2);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof AddColumnStatement);
        AddColumnStatement stmt = (AddColumnStatement)statements[0];
        Assertions.assertTrue(stmt.isMultiple());
        Assertions.assertEquals(2, stmt.getColumns().size());
    }

    @Test
    public void generateStatements_DB2_multipleColumns_single_reorg() {
        AddColumnChange change = new AddColumnChange();
        AddColumnConfig column1 = new AddColumnConfig();
        column1.setName("column1");
        column1.setType("INT");
        change.addColumn(column1);
        AddColumnConfig column2 = new AddColumnConfig();
        column2.setName("column2");
        column2.setType("INT");
        change.addColumn(column2);

        SqlStatement[] statements = change.generateStatements(new DB2Database());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof AddColumnStatement);
        AddColumnStatement stmt = (AddColumnStatement)statements[0];
        Assertions.assertTrue(stmt.isMultiple());
        Assertions.assertEquals(2, stmt.getColumns().size());
        Assertions.assertTrue(statements[1] instanceof ReorganizeTableStatement);

    }
}
