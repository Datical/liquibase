package liquibase.serializer.core.yaml;

import liquibase.serializer.SnapshotSerializer;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link YamlSnapshotSerializer}.
 *
 * Note: Tests involving DatabaseSnapshot serialization require a fully initialized
 * database connection which is not available in unit tests. Those scenarios are
 * better tested in integration tests.
 */
public class YamlSnapshotSerializerTest {

    private YamlSnapshotSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new YamlSnapshotSerializer();
    }

    @Test
    void getValidFileExtensions_default_returnsYamlAndYml() {
        String[] extensions = serializer.getValidFileExtensions();

        assertNotNull(extensions);
        assertEquals(2, extensions.length);
        assertEquals("yaml", extensions[0]);
        assertEquals("yml", extensions[1]);
    }

    @Test
    void getPriority_default_returnsDefaultPriority() {
        int priority = serializer.getPriority();

        assertEquals(SnapshotSerializer.PRIORITY_DEFAULT, priority);
    }

    @Test
    void serialize_withTableObject_returnsYamlContainingTable() {
        Table table = new Table();
        table.setName("TEST_TABLE");
        table.setSnapshotId("table-123");

        String result = serializer.serialize(table, false);

        assertNotNull(result);
        assertTrue(result.contains("table"));
        assertTrue(result.contains("TEST_TABLE"));
    }

    @Test
    void serialize_withSchemaObject_returnsYamlContainingSchema() {
        Schema schema = new Schema();
        schema.setName("PUBLIC");
        schema.setSnapshotId("schema-456");

        String result = serializer.serialize(schema, false);

        assertNotNull(result);
        assertTrue(result.contains("schema"));
    }

    @Test
    void serialize_withCatalogObject_returnsYamlContainingCatalog() {
        Catalog catalog = new Catalog();
        catalog.setName("TEST_CATALOG");
        catalog.setSnapshotId("catalog-789");

        String result = serializer.serialize(catalog, false);

        assertNotNull(result);
        assertTrue(result.contains("catalog"));
    }

    @Test
    void serialize_withPrettyOption_returnsOutput() {
        Table table = new Table();
        table.setName("TEST_TABLE");
        table.setSnapshotId("table-001");

        String prettyResult = serializer.serialize(table, true);
        String normalResult = serializer.serialize(table, false);

        assertNotNull(prettyResult);
        assertNotNull(normalResult);
        assertTrue(prettyResult.contains("table"));
        assertTrue(normalResult.contains("table"));
    }

    @Test
    void serialize_withTableAndSchema_returnsYamlContainingBoth() {
        Schema schema = new Schema();
        schema.setName("MY_SCHEMA");
        schema.setSnapshotId("schema-100");

        Table table = new Table();
        table.setName("USERS");
        table.setSnapshotId("table-200");
        table.setSchema(schema);

        String result = serializer.serialize(table, false);

        assertNotNull(result);
        assertTrue(result.contains("table"));
        assertTrue(result.contains("USERS"));
    }

    @Test
    void serialize_withMultipleTables_returnsYamlForEach() {
        Table table1 = new Table();
        table1.setName("TABLE_ONE");
        table1.setSnapshotId("t1");

        Table table2 = new Table();
        table2.setName("TABLE_TWO");
        table2.setSnapshotId("t2");

        String result1 = serializer.serialize(table1, false);
        String result2 = serializer.serialize(table2, false);

        assertNotNull(result1);
        assertNotNull(result2);
        assertTrue(result1.contains("TABLE_ONE"));
        assertTrue(result2.contains("TABLE_TWO"));
    }

    @Test
    void serialize_withNullObject_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> {
            serializer.serialize(null, false);
        });
    }
}
