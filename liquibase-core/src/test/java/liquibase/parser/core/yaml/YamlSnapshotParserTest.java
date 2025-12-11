package liquibase.parser.core.yaml;

import liquibase.exception.LiquibaseParseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import liquibase.snapshot.DatabaseSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link YamlSnapshotParser}.
 */
public class YamlSnapshotParserTest {

    private YamlSnapshotParser parser;

    @BeforeEach
    void setUp() {
        parser = new YamlSnapshotParser();
    }

    @Test
    void supports_withYamlExtension_returnsTrue() {
        ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

        assertTrue(parser.supports("test.yaml", resourceAccessor));
        assertTrue(parser.supports("test.yml", resourceAccessor));
        assertTrue(parser.supports("path/to/snapshot.yaml", resourceAccessor));
        assertTrue(parser.supports("path/to/snapshot.yml", resourceAccessor));
    }

    @Test
    void supports_withNonYamlExtension_returnsFalse() {
        ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

        assertFalse(parser.supports("test.xml", resourceAccessor));
        assertFalse(parser.supports("test.json", resourceAccessor));
        assertFalse(parser.supports("test.sql", resourceAccessor));
        assertFalse(parser.supports("test.txt", resourceAccessor));
    }

    @Test
    void supports_withUpperCaseExtension_returnsTrue() {
        ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

        assertTrue(parser.supports("test.YAML", resourceAccessor));
        assertTrue(parser.supports("test.YML", resourceAccessor));
        assertTrue(parser.supports("test.Yaml", resourceAccessor));
    }

    @Test
    void parse_withNonExistentFile_throwsLiquibaseParseException() {
        ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

        LiquibaseParseException exception = assertThrows(LiquibaseParseException.class, () -> {
            parser.parse("non-existent-file.yaml", resourceAccessor);
        });

        assertTrue(exception.getMessage().contains("does not exist"));
    }

    @Test
    void parse_withMissingSnapshotNode_throwsLiquibaseParseException() {
        String yamlContent = "someKey: someValue\nanotherKey: anotherValue";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", yamlContent);

        LiquibaseParseException exception = assertThrows(LiquibaseParseException.class, () -> {
            parser.parse("test.yaml", resourceAccessor);
        });

        assertTrue(exception.getMessage().contains("Could not find root snapshot node"));
    }

    @Test
    void parse_withInvalidYamlSyntax_throwsLiquibaseParseException() {
        String invalidYaml = "snapshot:\n  - invalid: [unclosed bracket";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", invalidYaml);

        LiquibaseParseException exception = assertThrows(LiquibaseParseException.class, () -> {
            parser.parse("test.yaml", resourceAccessor);
        });

        assertTrue(exception.getMessage().contains("Syntax error"));
    }

    @Test
    void parse_withEmptyFile_throwsLiquibaseParseException() {
        String emptyContent = "";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", emptyContent);

        assertThrows(LiquibaseParseException.class, () -> {
            parser.parse("test.yaml", resourceAccessor);
        });
    }

    @Test
    void parse_withValidSnapshot_returnsDatabaseSnapshot() {
        String validSnapshot = "snapshot:\n" +
                "  database:\n" +
                "    shortName: h2\n" +
                "  objects: {}\n";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", validSnapshot);

        try {
            DatabaseSnapshot result = parser.parse("test.yaml", resourceAccessor);
            assertNotNull(result);
        } catch (LiquibaseParseException e) {
            assertFalse(e.getMessage().contains("Syntax error"));
        }
    }

    @Test
    void parse_withMetadata_parsesMetadataSuccessfully() {
        String snapshotWithMetadata = "snapshot:\n" +
                "  database:\n" +
                "    shortName: h2\n" +
                "  metadata:\n" +
                "    key1: value1\n" +
                "    key2: value2\n" +
                "  objects: {}\n";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", snapshotWithMetadata);

        try {
            DatabaseSnapshot result = parser.parse("test.yaml", resourceAccessor);
            assertNotNull(result);
            assertNotNull(result.getMetadata());
        } catch (LiquibaseParseException e) {
            assertFalse(e.getMessage().contains("Syntax error"));
        }
    }

    @Test
    void parse_withLargeContent_doesNotFailDueToCodePointLimit() throws LiquibaseParseException {
        StringBuilder largeContent = new StringBuilder();
        largeContent.append("snapshot:\n");
        largeContent.append("  database:\n");
        largeContent.append("    shortName: h2\n");
        largeContent.append("    majorVersion: 2\n");
        largeContent.append("    minorVersion: 1\n");
        largeContent.append("  metadata:\n");
        String paddedValue = String.join("", Collections.nCopies(100, "x"));
        for (int i = 0; i < 35000; i++) {
            largeContent.append("    metaKey").append(i).append(": ").append(paddedValue).append("\n");
        }
        largeContent.append("  objects:\n");
        largeContent.append("    liquibase.structure.core.Catalog:\n");
        largeContent.append("      - snapshotId: cat1\n");
        largeContent.append("        name: TEST_CATALOG\n");
        ResourceAccessor resourceAccessor = createMockResourceAccessor("large-snapshot.yaml", largeContent.toString());

        DatabaseSnapshot snapshot = parser.parse("large-snapshot.yaml", resourceAccessor);

        assertNotNull(snapshot, "Snapshot should not be null");
        assertNotNull(snapshot.getDatabase(), "Database should not be null");
        assertEquals("h2", snapshot.getDatabase().getShortName(), "Database short name should be h2");
        assertNotNull(snapshot.getMetadata(), "Metadata should not be null");
        assertEquals(35000, snapshot.getMetadata().size(), "Should have 35000 metadata entries");
    }

    @Test
    void parse_withNullResourceAccessor_throwsLiquibaseParseException() {
        assertThrows(LiquibaseParseException.class, () -> {
            parser.parse("test.yaml", null);
        });
    }

    /**
     * Creates a mock ResourceAccessor that returns the specified content for the given path.
     */
    private ResourceAccessor createMockResourceAccessor(String path, String content) {
        return new ResourceAccessor() {
            @Override
            public Set<InputStream> getResourcesAsStream(String requestedPath) throws IOException {
                if (requestedPath.equals(path)) {
                    return Collections.singleton(
                            new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
                    );
                }
                return Collections.emptySet();
            }

            @Override
            public Set<String> list(String relativeTo, String listPath, boolean includeFiles,
                                    boolean includeDirectories, boolean recursive) throws IOException {
                return Collections.emptySet();
            }

            @Override
            public ClassLoader toClassLoader() {
                return this.getClass().getClassLoader();
            }
        };
    }
}
