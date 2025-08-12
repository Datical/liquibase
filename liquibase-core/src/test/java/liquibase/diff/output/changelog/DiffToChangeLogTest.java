package liquibase.diff.output.changelog;

import liquibase.database.core.MySQLDatabase;
import liquibase.diff.DiffResult;
import liquibase.diff.compare.CompareControl;
import liquibase.snapshot.EmptyDatabaseSnapshot;
import liquibase.structure.DatabaseObject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class DiffToChangeLogTest {

    @Test
    public void getOrderedOutputTypes_isConsistant() throws Exception {
        MySQLDatabase database = new MySQLDatabase();
        DiffToChangeLog obj = new DiffToChangeLog(new DiffResult(new EmptyDatabaseSnapshot(database), new EmptyDatabaseSnapshot(database), new CompareControl()), null);

        for (Class<? extends ChangeGenerator> type : new Class[] {UnexpectedObjectChangeGenerator.class, MissingObjectChangeGenerator.class, ChangedObjectChangeGenerator.class}) {
            List<Class<? extends DatabaseObject>> orderedOutputTypes = obj.getOrderedOutputTypes(type);
            assertArrayEquals(orderedOutputTypes.toArray(), obj.getOrderedOutputTypes(type).toArray(), "Error checking " + type.getName());
        }
    }
}
