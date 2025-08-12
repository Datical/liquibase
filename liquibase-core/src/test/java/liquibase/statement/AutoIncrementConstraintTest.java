package liquibase.statement;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AutoIncrementConstraintTest {
    
    @Test
    public void ctor() {
        AutoIncrementConstraint constraint = new AutoIncrementConstraint("COL_NAME");
        assertEquals("COL_NAME", constraint.getColumnName());
    }
}
