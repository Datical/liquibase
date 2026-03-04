package liquibase.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link DuplicateStatementIdentifierException}
 */
@SuppressWarnings({"ThrowableInstanceNeverThrown"})
public class DuplicateStatementIdentifierExceptionTest {

    @Test
    public void duplicateStatementIdentifierException() throws Exception {
        DuplicateStatementIdentifierException ex = new DuplicateStatementIdentifierException("Message Here");
        assertEquals("Message Here", ex.getMessage());
    }
}