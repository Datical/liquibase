package liquibase.logging.jvm;

import liquibase.logging.LogFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LogFactoryTest {
    
    @Test
    public void getLogger() {
        assertNotNull(LogFactory.getLogger());
    }
}
