package liquibase.dbtest.mssql;

import liquibase.CatalogAndSchema;
import liquibase.Liquibase;
import liquibase.database.core.MSSQLDatabase;
import liquibase.datatype.DataTypeFactory;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.statement.DatabaseFunction;
import liquibase.structure.core.Column;
import liquibase.structure.core.Table;
import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

public class MssqlIntegrationTest extends AbstractMssqlIntegrationTest {

    public MssqlIntegrationTest() throws Exception {
        super("Mssql", "jdbc:sqlserver://"+ getDatabaseServerHostname("Mssql") +":1433;databaseName=liquibase");
    }

    @Override
    protected boolean supportsAltCatalogTests() {
        return false;
    }

    @Test
    public void defaultValuesTests() throws Exception {
        if (this.getDatabase() == null) {
            return;
        }

        Liquibase liquibase = createLiquibase("changelogs/mssql/issues/default.values.xml");
        liquibase.update((String) null);

        DatabaseSnapshot snapshot = SnapshotGeneratorFactory.getInstance().createSnapshot(CatalogAndSchema.DEFAULT, this.getDatabase(), new SnapshotControl(getDatabase()));

        for (Table table : snapshot.get(Table.class)) {
            for (Column column : table.getColumns()) {
                if (column.getName().toLowerCase().endsWith("_default")) {
                    Object defaultValue = column.getDefaultValue();
                    assertNotNull(defaultValue, "Null default value for " + table.getName() + "." + column.getName());
                    if (column.getName().toLowerCase().contains("date") || column.getName().toLowerCase().contains("time")) {
                        if (defaultValue instanceof DatabaseFunction) {
                            ((DatabaseFunction) defaultValue).getValue().contains("type datetimeoffset");
                        } else {
                            assertTrue(defaultValue instanceof Date, "Unexpected default type " + defaultValue.getClass().getName() + " for " + table.getName() + "." + column.getName());
                            Calendar calendar = Calendar.getInstance();
                            calendar.setTime(((Date) defaultValue));
                            assertEquals(1, calendar.get(Calendar.DAY_OF_MONTH));
                            assertEquals(1, calendar.get(Calendar.MONTH));
                            assertEquals(2000, calendar.get(Calendar.YEAR));
                        }
                    } else if (column.getName().toLowerCase().contains("char_")) {
                        assertTrue(defaultValue instanceof String, "Unexpected default type " + defaultValue.getClass().getName() + " for " + table.getName() + "." + column.getName());
                    } else if (column.getName().toLowerCase().contains("binary_")) {
                        assertTrue(defaultValue instanceof DatabaseFunction, "Unexpected default type " + defaultValue.getClass().getName() + " for " + table.getName() + "." + column.getName());
                    } else {
                        assertTrue(defaultValue instanceof Number, "Unexpected default type " + defaultValue.getClass().getName() + " for " + table.getName() + "." + column.getName());
                        assertEquals(1, ((Number) defaultValue).intValue());
                    }
                }
            }
        }
    }

    @Test
    public void dataTypesTest() throws Exception {
        if (this.getDatabase() == null) {
            return;
        }

        Liquibase liquibase = createLiquibase("changelogs/mssql/issues/data.types.xml");
        liquibase.update((String) null);

        DatabaseSnapshot snapshot = SnapshotGeneratorFactory.getInstance().createSnapshot(CatalogAndSchema.DEFAULT, this.getDatabase(), new SnapshotControl(getDatabase()));

        for (Table table : snapshot.get(Table.class)) {
            if (getDatabase().isLiquibaseObject(table)) {
                continue;
            }
            for (Column column : table.getColumns()) {
                String expectedType = column.getName().split("_")[0];

                if (expectedType.equalsIgnoreCase("text")) {
                    expectedType = "nvarchar";
                }

                String foundTypeDefinition = DataTypeFactory.getInstance().from(column.getType(), new MSSQLDatabase()).toDatabaseDataType(getDatabase()).toString();
                String foundType = foundTypeDefinition.replaceFirst("\\(.*", "");
                assertEquals(expectedType.toLowerCase(), foundType.toLowerCase(), "Wrong data type for " + table.getName() + "." + column.getName());

                if (expectedType.equalsIgnoreCase("varbinary")) {
                    if (column.getName().endsWith("_MAX")) {
                        assertEquals("VARBINARY(MAX)", foundTypeDefinition);
                    } else {
                        assertEquals("VARBINARY(1)", foundTypeDefinition);
                    }
                }
            }
        }
    }


    @Test
    public void dataTypeParamsTest() throws Exception {
        if (this.getDatabase() == null) {
            return;
        }

        Liquibase liquibase = createLiquibase("changelogs/mssql/issues/data.type.params.xml");
        liquibase.update((String) null);

        DatabaseSnapshot snapshot = SnapshotGeneratorFactory.getInstance().createSnapshot(CatalogAndSchema.DEFAULT, this.getDatabase(), new SnapshotControl(getDatabase()));

        for (Table table : snapshot.get(Table.class)) {
            if (getDatabase().isLiquibaseObject(table)) {
                continue;
            }
            for (Column column : table.getColumns()) {
                String expectedType = column.getName().split("_")[0];

                String foundTypeDefinition = DataTypeFactory.getInstance().from(column.getType(), new MSSQLDatabase()).toDatabaseDataType(getDatabase()).toString();

                assertFalse(foundTypeDefinition.contains("("), "Parameter found in " + table.getName() + "." + column.getName());
            }
        }
    }
}
