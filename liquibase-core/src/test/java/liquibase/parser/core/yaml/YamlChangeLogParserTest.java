package liquibase.parser.core.yaml;

import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.ChangeLogParseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
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
 * Unit tests for {@link YamlChangeLogParser}.
 */
public class YamlChangeLogParserTest {

    private YamlChangeLogParser parser;
    private ChangeLogParameters changeLogParameters;

    @BeforeEach
    void setUp() {
        parser = new YamlChangeLogParser();
        changeLogParameters = new ChangeLogParameters();
    }

    @Test
    void supports_withYamlExtension_returnsTrue() {
        ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

        assertTrue(parser.supports("test.yaml", resourceAccessor));
        assertTrue(parser.supports("test.yml", resourceAccessor));
        assertTrue(parser.supports("path/to/changelog.yaml", resourceAccessor));
        assertTrue(parser.supports("path/to/changelog.yml", resourceAccessor));
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
    void parse_withNonExistentFile_throwsChangeLogParseException() {
        ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

        ChangeLogParseException exception = assertThrows(ChangeLogParseException.class, () -> {
            parser.parse("non-existent-file.yaml", changeLogParameters, resourceAccessor);
        });

        assertTrue(exception.getMessage().contains("does not exist"));
    }

    @Test
    void parse_withEmptyFile_throwsChangeLogParseException() {
        String emptyContent = "";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", emptyContent);

        assertThrows(ChangeLogParseException.class, () -> {
            parser.parse("test.yaml", changeLogParameters, resourceAccessor);
        });
    }

    @Test
    void parse_withMissingDatabaseChangeLogNode_throwsChangeLogParseException() {
        String yamlContent = "someKey: someValue\nanotherKey: anotherValue";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", yamlContent);

        ChangeLogParseException exception = assertThrows(ChangeLogParseException.class, () -> {
            parser.parse("test.yaml", changeLogParameters, resourceAccessor);
        });

        assertTrue(exception.getMessage().contains("databaseChangeLog"));
    }

    @Test
    void parse_withInvalidYamlSyntax_throwsChangeLogParseException() {
        String invalidYaml = "databaseChangeLog:\n  - invalid: [unclosed bracket";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", invalidYaml);

        ChangeLogParseException exception = assertThrows(ChangeLogParseException.class, () -> {
            parser.parse("test.yaml", changeLogParameters, resourceAccessor);
        });

        assertTrue(exception.getMessage().contains("Syntax error"));
    }

    @Test
    void parse_withDatabaseChangeLogNotAList_throwsChangeLogParseException() {
        String yamlContent = "databaseChangeLog: notAList";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", yamlContent);

        assertThrows(ChangeLogParseException.class, () -> {
            parser.parse("test.yaml", changeLogParameters, resourceAccessor);
        });
    }

    @Test
    void parse_withValidEmptyChangeLog_returnsDatabaseChangeLog() throws Exception {
        String validChangeLog = "databaseChangeLog: []";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", validChangeLog);

        DatabaseChangeLog result = parser.parse("test.yaml", changeLogParameters, resourceAccessor);

        assertNotNull(result);
    }

    @Test
    void parse_withValidChangeLog_setsPhysicalFilePath() throws Exception {
        String validChangeLog = "databaseChangeLog: []";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("path/to/changelog.yaml", validChangeLog);

        DatabaseChangeLog result = parser.parse("path/to/changelog.yaml", changeLogParameters, resourceAccessor);

        assertNotNull(result);
        assertEquals("path/to/changelog.yaml", result.getPhysicalFilePath());
    }

    @Test
    void parse_withSingleProperty_parsesPropertySuccessfully() throws Exception {
        String changeLogWithProperty = "databaseChangeLog:\n" +
                "  - property:\n" +
                "      name: testProp\n" +
                "      value: testValue";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", changeLogWithProperty);

        DatabaseChangeLog result = parser.parse("test.yaml", changeLogParameters, resourceAccessor);

        assertNotNull(result);
    }

    @Test
    void parse_withMultipleProperties_parsesAllProperties() throws Exception {
        String changeLogWithProperties = "databaseChangeLog:\n" +
                "  - property:\n" +
                "      name: prop1\n" +
                "      value: value1\n" +
                "  - property:\n" +
                "      name: prop2\n" +
                "      value: value2";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", changeLogWithProperties);

        DatabaseChangeLog result = parser.parse("test.yaml", changeLogParameters, resourceAccessor);

        assertNotNull(result);
    }

    @Test
    void parse_withPropertyGlobalTrue_parsesSuccessfully() throws Exception {
        String changeLogWithGlobalProperty = "databaseChangeLog:\n" +
                "  - property:\n" +
                "      name: globalProp\n" +
                "      value: globalValue\n" +
                "      global: true";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", changeLogWithGlobalProperty);

        DatabaseChangeLog result = parser.parse("test.yaml", changeLogParameters, resourceAccessor);

        assertNotNull(result);
    }

    @Test
    void parse_withPropertyGlobalFalse_parsesSuccessfully() throws Exception {
        String changeLogWithLocalProperty = "databaseChangeLog:\n" +
                "  - property:\n" +
                "      name: localProp\n" +
                "      value: localValue\n" +
                "      global: false";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", changeLogWithLocalProperty);

        DatabaseChangeLog result = parser.parse("test.yaml", changeLogParameters, resourceAccessor);

        assertNotNull(result);
    }

    @Test
    void parse_withPropertyContext_parsesSuccessfully() throws Exception {
        String changeLogWithContextProperty = "databaseChangeLog:\n" +
                "  - property:\n" +
                "      name: contextProp\n" +
                "      value: contextValue\n" +
                "      context: test";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", changeLogWithContextProperty);

        DatabaseChangeLog result = parser.parse("test.yaml", changeLogParameters, resourceAccessor);

        assertNotNull(result);
    }

    @Test
    void parse_withPropertyLabels_parsesSuccessfully() throws Exception {
        String changeLogWithLabelsProperty = "databaseChangeLog:\n" +
                "  - property:\n" +
                "      name: labeledProp\n" +
                "      value: labeledValue\n" +
                "      labels: dev";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", changeLogWithLabelsProperty);

        DatabaseChangeLog result = parser.parse("test.yaml", changeLogParameters, resourceAccessor);

        assertNotNull(result);
    }

    @Test
    void parse_withLargeContent_doesNotFailDueToCodePointLimit() throws ChangeLogParseException {
        StringBuilder largeContent = new StringBuilder();
        largeContent.append("databaseChangeLog:\n");
        String paddedValue = String.join("", Collections.nCopies(100, "x"));
        for (int i = 0; i < 35000; i++) {
            largeContent.append("  - property:\n");
            largeContent.append("      name: prop").append(i).append("\n");
            largeContent.append("      value: ").append(paddedValue).append("\n");
        }
        ResourceAccessor resourceAccessor = createMockResourceAccessor("large-changelog.yaml", largeContent.toString());

        DatabaseChangeLog changeLog = parser.parse("large-changelog.yaml", changeLogParameters, resourceAccessor);

        assertNotNull(changeLog, "ChangeLog should not be null");
        assertEquals("large-changelog.yaml", changeLog.getPhysicalFilePath(), "Physical file path should match");
    }

    @Test
    void parse_withNullChangeLogParameters_handlesGracefully() throws Exception {
        String validChangeLog = "databaseChangeLog: []";
        ResourceAccessor resourceAccessor = createMockResourceAccessor("test.yaml", validChangeLog);

        DatabaseChangeLog result = parser.parse("test.yaml", null, resourceAccessor);

        assertNotNull(result);
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
