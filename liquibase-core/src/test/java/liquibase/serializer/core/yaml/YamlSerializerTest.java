package liquibase.serializer.core.yaml;

import liquibase.change.ColumnConfig;
import liquibase.change.core.CreateTableChange;
import liquibase.changelog.ChangeSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link YamlSerializer} abstract class functionality.
 * Tests are performed through concrete implementation {@link YamlChangeLogSerializer}.
 */
public class YamlSerializerTest {

    @Test
    void getValidFileExtensions_default_returnsYamlAndYml() {
        YamlChangeLogSerializer serializer = new YamlChangeLogSerializer();

        String[] extensions = serializer.getValidFileExtensions();

        assertNotNull(extensions);
        assertEquals(2, extensions.length);
        assertEquals("yaml", extensions[0]);
        assertEquals("yml", extensions[1]);
    }

    @Test
    void serialize_withChangeSet_returnsYamlString() {
        YamlChangeLogSerializer serializer = new YamlChangeLogSerializer();
        ChangeSet changeSet = new ChangeSet("test1", "testAuthor", false, true, "/test/path.xml", null, null, null);
        CreateTableChange change = new CreateTableChange();
        change.setTableName("testTable");
        change.addColumn(new ColumnConfig().setName("id").setType("int"));
        changeSet.addChange(change);

        String result = serializer.serialize(changeSet, false);

        assertNotNull(result);
        assertTrue(result.contains("changeSet"));
        assertTrue(result.contains("testTable"));
        assertTrue(result.contains("id"));
    }

    @Test
    void serialize_withPrettyPrintOption_returnsOutput() {
        YamlChangeLogSerializer serializer = new YamlChangeLogSerializer();
        ChangeSet changeSet = new ChangeSet("test1", "testAuthor", false, true, "/test/path.xml", null, null, null);
        CreateTableChange change = new CreateTableChange();
        change.setTableName("testTable");
        changeSet.addChange(change);

        String prettyResult = serializer.serialize(changeSet, true);
        String normalResult = serializer.serialize(changeSet, false);

        assertNotNull(prettyResult);
        assertNotNull(normalResult);
    }

    @Test
    void serialize_withEmptyChangeSet_returnsYamlWithChangeSet() {
        YamlChangeLogSerializer serializer = new YamlChangeLogSerializer();
        ChangeSet changeSet = new ChangeSet("empty", "testAuthor", false, true, "/test/path.xml", null, null, null);

        String result = serializer.serialize(changeSet, false);

        assertNotNull(result);
        assertTrue(result.contains("changeSet"));
    }

    @Test
    void serialize_withMultipleColumns_returnsAllColumnsInYaml() {
        YamlChangeLogSerializer serializer = new YamlChangeLogSerializer();
        ChangeSet changeSet = new ChangeSet("multi", "testAuthor", false, true, "/test/path.xml", null, null, null);
        CreateTableChange change = new CreateTableChange();
        change.setTableName("users");
        change.addColumn(new ColumnConfig().setName("id").setType("int"));
        change.addColumn(new ColumnConfig().setName("username").setType("varchar(100)"));
        change.addColumn(new ColumnConfig().setName("email").setType("varchar(255)"));
        changeSet.addChange(change);

        String result = serializer.serialize(changeSet, false);

        assertNotNull(result);
        assertTrue(result.contains("users"));
        assertTrue(result.contains("id"));
        assertTrue(result.contains("username"));
        assertTrue(result.contains("email"));
    }

    @Test
    void getValidFileExtensions_default_doesNotReturnJson() {
        YamlChangeLogSerializer serializer = new YamlChangeLogSerializer();

        String[] extensions = serializer.getValidFileExtensions();

        assertNotEquals("json", extensions[0]);
    }
}
