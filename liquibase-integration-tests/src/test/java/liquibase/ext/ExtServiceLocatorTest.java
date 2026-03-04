package liquibase.ext;

import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import liquibase.servicelocator.ServiceLocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class ExtServiceLocatorTest {
    private ServiceLocator serviceLocator;

    @BeforeEach
    public void setup() throws Exception{
        ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

        serviceLocator = ServiceLocator.getInstance();
        serviceLocator.setResourceAccessor(resourceAccessor);
    }

    @AfterEach
    public void teardown() {
        ServiceLocator.reset();
    }
}
