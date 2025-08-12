package liquibase.parser.core.xml;

import liquibase.resource.ResourceAccessor;
import liquibase.serializer.LiquibaseSerializer;
import liquibase.util.StreamUtil;
import liquibase.util.file.FilenameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class LiquibaseEntityResolverTest {

	private static final String SYSTEM_ID = "http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.1.xsd";

	private static final String SYSTEM_ID_WITH_MIGRATOR_PATH = "http://www.liquibase.org/xml/ns/migrator/dbchangelog-1.0.xsd";
	private static final String SYSTEM_ID_FROM_MIGRATOR_PATH = "http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-1.0.xsd";

	private static final String PUBLIC_ID = "publicId";
	private static final String NAME = "name";

	private static final String BASE_URI = "baseUri";
	private static final String BASE_PATH = "basePath";
	private static final String FILE_SYSTEM_ID = "fileSystemId";
	private static final String PATH_AND_SYSTEM_ID = FilenameUtils.concat(BASE_PATH, FILE_SYSTEM_ID);

	private LiquibaseEntityResolver liquibaseEntityResolver;

	@Mock
	private LiquibaseSchemaResolver liquibaseSchemaResolver;

	@Mock
	private ResourceAccessor resourceAccessor;

	@Mock
	private InputSource inputSource;

	@Mock
	private XMLChangeLogSAXParser parser;

	@Mock
	private InputStream inputStream;

	@Mock
	private LiquibaseSerializer serializer;
	private MockedStatic<StreamUtil> streamUtilMockedStatic;
	private MockedConstruction<LiquibaseSchemaResolver> liquibaseSchemaResolverMockedConstruction;

	@BeforeEach
	public void setUp() throws Exception {
		streamUtilMockedStatic = mockStatic(StreamUtil.class);
		liquibaseSchemaResolverMockedConstruction = mockConstruction(
				LiquibaseSchemaResolver.class,
				(mock, context) -> {
					if (context.arguments().equals(List.of(SYSTEM_ID, PUBLIC_ID, resourceAccessor))) {
						when(mock.resolve(parser)).thenReturn(inputSource);
						when(mock.resolve(serializer)).thenReturn(inputSource);
					}
				});

		liquibaseEntityResolver = new LiquibaseEntityResolver(parser);
		liquibaseEntityResolver.useResoureAccessor(resourceAccessor, BASE_PATH);
	}

	@AfterEach
	public void tearDown() {
		streamUtilMockedStatic.close();
		liquibaseSchemaResolverMockedConstruction.close();
	}

	@Test
	public void shouldReturnNullSystemIdIsNull() throws Exception {
		InputSource result = liquibaseEntityResolver.resolveEntity(NAME, PUBLIC_ID, BASE_URI, null);

		assertThat(result).isNull();
	}

	@Test
	void systemIdStartingWithMigratorShouldBeReplacedByDbChangelog() throws Exception {
		liquibaseSchemaResolverMockedConstruction.close();
		liquibaseSchemaResolverMockedConstruction = mockConstruction(
				LiquibaseSchemaResolver.class,
				(mock, context) -> {
					if (context.arguments().equals(
							List.of(SYSTEM_ID_FROM_MIGRATOR_PATH, PUBLIC_ID, resourceAccessor))) {
						when(mock.resolve(parser)).thenReturn(inputSource);
						when(mock.resolve(serializer)).thenReturn(inputSource);
					}
				});

			InputSource result = liquibaseEntityResolver.resolveEntity(NAME, PUBLIC_ID, BASE_URI, SYSTEM_ID_WITH_MIGRATOR_PATH);

			assertThat(result).isSameAs(inputSource);
	}

	@Test
	public void shouldReturnSchemaResolverResultWhenSystemIdIsValidXsd() throws IOException, SAXException {
		InputSource result = liquibaseEntityResolver.resolveEntity(NAME, PUBLIC_ID, BASE_URI, SYSTEM_ID);

		assertThat(result).isSameAs(inputSource);
	}

	@Test
	public void shouldReturnSchemaResolverResultWhenSystemIdIsValidXsdAndSerializerIsNotNull() throws IOException, SAXException {
		liquibaseEntityResolver = new LiquibaseEntityResolver(serializer);
		liquibaseEntityResolver.useResoureAccessor(resourceAccessor, BASE_PATH);

		InputSource result = liquibaseEntityResolver.resolveEntity(NAME, PUBLIC_ID, BASE_URI, SYSTEM_ID);

		assertThat(result).isSameAs(inputSource);
	}

	@Test
	public void whenSystemIdIsNotXsdLoadResourceFromBasepathWithResourceAccessor() throws IOException, SAXException {
		streamUtilMockedStatic.when(() -> StreamUtil.singleInputStream(PATH_AND_SYSTEM_ID, resourceAccessor)).thenReturn(inputStream);

		InputSource result = liquibaseEntityResolver.resolveEntity(NAME, PUBLIC_ID, BASE_URI, FILE_SYSTEM_ID);

		assertThat(result.getByteStream()).isEqualTo(inputStream);
	}

	@Test
	public void whenSystemIdIsNotXsdAndResourceCouldNotBeLoadedFromResourceAccessorReturnNull() throws IOException, SAXException {
		streamUtilMockedStatic.when(() -> StreamUtil.singleInputStream(PATH_AND_SYSTEM_ID, resourceAccessor)).thenReturn(null);

		InputSource result = liquibaseEntityResolver.resolveEntity(NAME, PUBLIC_ID, BASE_URI, FILE_SYSTEM_ID);

		assertThat(result).isEqualTo(null);
	}

	@Test
	public void whenSystemIdIsNotXsdAndResourceExceptionOccursReturnNull() throws IOException, SAXException {
		streamUtilMockedStatic.when(() -> StreamUtil.singleInputStream(PATH_AND_SYSTEM_ID, resourceAccessor)).thenThrow(new RuntimeException());

		InputSource result = liquibaseEntityResolver.resolveEntity(NAME, PUBLIC_ID, BASE_URI, FILE_SYSTEM_ID);

		assertThat(result).isEqualTo(null);
	}

	@Test
	public void resolveEntityWithOnlyPublicIdAndSystemIdDelegatesToSchemResolver() throws IOException, SAXException {
		InputSource result = liquibaseEntityResolver.resolveEntity(PUBLIC_ID, SYSTEM_ID);

		assertThat(result).isSameAs(inputSource);
	}

	@Test
	public void getExternalSubsetShouldReturnNull() throws IOException, SAXException {
		InputSource externalSubset = liquibaseEntityResolver.getExternalSubset(NAME, BASE_URI);

		assertThat(externalSubset).isNull();
	}
}