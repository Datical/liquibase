package liquibase.parser.core.xml;

import liquibase.resource.ResourceAccessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ContextClassLoaderXsdStreamResolverTest {

	private static final String EXISTING_XSD_FILE = "liquibase/parser/core/xml/unused.xsd";
	private static final String NON_EXISTING_XSD_FILE = "xsdFile";

	@InjectMocks
	private ContextClassLoaderXsdStreamResolver contextClassLoaderXsdStreamResolver;

	@Mock
	private XsdStreamResolver successor;

	@Mock
	private ResourceAccessor resourceAccessor;

	@Mock
	private InputStream successorValue;

	@BeforeEach
	public void setUp() {
		contextClassLoaderXsdStreamResolver.setSuccessor(successor);

		when(successor.getResourceAsStream(NON_EXISTING_XSD_FILE)).thenReturn(successorValue);
	}

	@Test
	public void whenResourceStreamIsNotNullThenReturnStream() throws IOException {
		InputStream returnValue = contextClassLoaderXsdStreamResolver.getResourceAsStream(EXISTING_XSD_FILE);

		assertThat(returnValue).isInstanceOf(InputStream.class);
	}

	@Test
	public void whenContextClassLoaderIsNullThenReturnSuccessorValue() throws IOException {
		InputStream returnValue = contextClassLoaderXsdStreamResolver.getResourceAsStream(NON_EXISTING_XSD_FILE);

		assertThat(returnValue).isSameAs(successorValue);
	}

}