package liquibase.util.file;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Petr Kozelka
 */
public class FilenameUtilsTest {

    /**
     * This checks that {@link FilenameUtils#concat} works fine with <code>basePath=null</code>.
     * <p>See <a href="https://liquibase.jira.com/browse/CORE-2385">issue CORE-2385</a>.</p>
     */
    @Test
    public void concatWithNullBasePath() {
        final String something = "liquibase/delta-changelogs/";
        Assertions.assertEquals(FilenameUtils.concat(null, something), something, "null basePath must not kill the result of concatenation");
    }
}
