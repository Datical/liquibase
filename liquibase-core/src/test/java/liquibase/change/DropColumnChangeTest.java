package liquibase.change;

import liquibase.change.core.DropColumnChange;
import liquibase.sdk.database.MockDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.DropColumnStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DropColumnChangeTest {

    @Test
    public void generateStatements_multipleColumns() {
        DropColumnChange change = new DropColumnChange();
        ColumnConfig column1 = new ColumnConfig();
        column1.setName("column1");
        change.addColumn(column1);
        ColumnConfig column2 = new ColumnConfig();
        column2.setName("column2");
        change.addColumn(column2);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DropColumnStatement);
        DropColumnStatement stmt = (DropColumnStatement)statements[0];
        Assertions.assertTrue(stmt.isMultiple());
        Assertions.assertEquals(2, stmt.getColumns().size());
    }
}
