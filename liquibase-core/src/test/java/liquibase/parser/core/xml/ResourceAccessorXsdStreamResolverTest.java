package liquibase.parser.core.xml;

import liquibase.resource.ResourceAccessor;
import liquibase.util.StreamUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ResourceAccessorXsdStreamResolverTest {

	private static final String XSD_FILE = "xsdFile";

	@InjectMocks
	private ResourceAccessorXsdStreamResolver resourceAccessorXsdStreamResolver;

	@Mock
	private XsdStreamResolver successor;

	@Mock
	private ResourceAccessor resourceAccessor;

	@Mock
	private InputStream inputStream, successorValue;

	private MockedStatic<StreamUtil> streamUtilMockedStatic;

	@BeforeEach
	public void setUp() throws IOException {
		streamUtilMockedStatic = mockStatic(StreamUtil.class);

		resourceAccessorXsdStreamResolver.setSuccessor(successor);

		when(successor.getResourceAsStream(XSD_FILE)).thenReturn(successorValue);
	}

	@AfterEach
	public void purgeStaticMocks(){
		streamUtilMockedStatic.close();
	}

	@Test
	public void whenResourceStreamIsNotNullThenReturnStream() throws IOException {
		streamUtilMockedStatic.when(() -> StreamUtil.singleInputStream(XSD_FILE, resourceAccessor)).thenReturn(inputStream);

		InputStream returnValue = resourceAccessorXsdStreamResolver.getResourceAsStream(XSD_FILE);

		assertThat(returnValue).isSameAs(inputStream);
	}

	@Test
	public void whenResourceStreamIsNullThenReturnSuccessorValue() throws IOException {
		streamUtilMockedStatic.when(() -> StreamUtil.singleInputStream(XSD_FILE, resourceAccessor)).thenReturn(null);

		InputStream returnValue = resourceAccessorXsdStreamResolver.getResourceAsStream(XSD_FILE);

		assertThat(returnValue).isSameAs(successorValue);
	}

	@Test
	public void whenIOExceptionOccursThenReturnSuccessorValue() throws IOException {
		streamUtilMockedStatic.when(() -> StreamUtil.singleInputStream(XSD_FILE, resourceAccessor)).thenThrow(new IOException());

		InputStream returnValue = resourceAccessorXsdStreamResolver.getResourceAsStream(XSD_FILE);

		assertThat(returnValue).isSameAs(successorValue);
	}
}