package liquibase.configuration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class LiquibaseConfigurationTest {

    @Test
    public void getContext_defaultSetup() {
        LiquibaseConfiguration liquibaseConfiguration = LiquibaseConfiguration.getInstance();
        GlobalConfiguration globalConfiguration = liquibaseConfiguration.getConfiguration(GlobalConfiguration.class);

        assertNotNull(globalConfiguration);

        assertSame(globalConfiguration, liquibaseConfiguration.getConfiguration(GlobalConfiguration.class), "Multiple calls to getConfiguration should return the same instance");
    }
}
