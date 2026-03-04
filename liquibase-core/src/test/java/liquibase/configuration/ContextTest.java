package liquibase.configuration;

import liquibase.exception.UnexpectedLiquibaseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ContextTest {

    private AbstractConfigurationContainer exampleConfiguration;

    @BeforeEach
    public void before() {
        System.clearProperty("liquibase.example.propertyBooleanNoDefault");
        System.clearProperty("liquibase.example.propertyBooleanDefaultFalse");
        System.clearProperty("liquibase.example.property.default.true");

        exampleConfiguration = new ExampleContext();
        exampleConfiguration.init(new SystemPropertyProvider());
    }

    @Test
    public void getValue() {
        assertNull(exampleConfiguration.getContainer().getValue("propertyBooleanNoDefault", Boolean.class));
        assertEquals(Boolean.TRUE, exampleConfiguration.getContainer().getValue("propertyBooleanDefaultTrue", Boolean.class));
        assertEquals(Boolean.FALSE, exampleConfiguration.getContainer().getValue("propertyBooleanDefaultFalse", Boolean.class));

    }

    @Test
    public void setValue_wrongType() {
        assertThrows(UnexpectedLiquibaseException.class, () -> exampleConfiguration.getContainer().setValue("propertyBooleanDefaultFalse", 124));
    }

    @Test
    public void getValue_defaultFromSystemProperties() {
        System.setProperty("liquibase.example.propertyBooleanNoDefault", "true");
        System.setProperty("liquibase.example.propertyBooleanDefaultFalse", "true");
        System.setProperty("liquibase.example.property.default.true", "false");
        ExampleContext exampleContext = new ExampleContext();
        exampleContext.init(new SystemPropertyProvider());

        assertTrue(exampleContext.getContainer().getValue("propertyBooleanNoDefault", Boolean.class));
        assertTrue(exampleContext.getContainer().getValue("propertyBooleanDefaultFalse", Boolean.class));
        assertFalse(exampleContext.getContainer().getValue("propertyBooleanDefaultTrue", Boolean.class));
    }

    private static class ExampleContext extends AbstractConfigurationContainer {
        private ExampleContext() {
            super("liquibase.example");

            getContainer().addProperty("propertyBooleanNoDefault", Boolean.class).setDescription("An example boolean property with no default");
            getContainer().addProperty("propertyBooleanDefaultTrue", Boolean.class).setDefaultValue(true).addAlias("property.default.true");
            getContainer().addProperty("propertyBooleanDefaultFalse", Boolean.class).setDefaultValue(false);
        }
    }


}
