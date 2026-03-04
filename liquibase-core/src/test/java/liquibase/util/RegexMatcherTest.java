package liquibase.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.regex.PatternSyntaxException;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author lujop
 */
public class RegexMatcherTest {
    private RegexMatcher matcher;
    private static final String text="Pulp Fiction\n" +
                "Reservoir Dogs\n" +
                "Kill Bill\n";

    @AfterEach
    public void tearDown() {
        matcher=null;
    }

    @Test
    public void testBadPatternFails() {
        assertThrows(PatternSyntaxException.class, () -> new RegexMatcher(text,new String[]{"a(j"}));
    }

    @Test
    public void testMatchingInSequentialOrder() {
        matcher=new RegexMatcher(text,new String[]{"Pulp","Reservoir","Kill"});
        assertTrue(matcher.allMatchedInSequentialOrder(), "All matched");

        matcher=new RegexMatcher(text,new String[]{"Pulp","ion"});
        assertTrue(matcher.allMatchedInSequentialOrder(), "All matched");

        matcher=new RegexMatcher(text,new String[]{"Pu.p","^Ki.+ll$"});
        assertTrue(matcher.allMatchedInSequentialOrder(), "All matched");

        matcher=new RegexMatcher(text,new String[]{"pulP","kiLL"});
        assertTrue(matcher.allMatchedInSequentialOrder(), "Case insensitive");

        matcher=new RegexMatcher(text,new String[]{"Reservoir","Pulp","Dogs"});
        assertFalse(matcher.allMatchedInSequentialOrder(), "Not in order");

        matcher=new RegexMatcher(text,new String[]{"Memento"});
        assertFalse(matcher.allMatchedInSequentialOrder(), "Not found");
    }

}