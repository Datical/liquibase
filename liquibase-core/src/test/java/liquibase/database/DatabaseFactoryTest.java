package liquibase.database;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DatabaseFactoryTest {
    
    @Test
    public void getInstance() {
        assertNotNull(DatabaseFactory.getInstance());
    }
}
