package liquibase.test;

import liquibase.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class Assert
{
    public static void assertSetsEqual(String[] expected, Set<String> set) {
        org.junit.jupiter.api.Assertions.assertEquals(expected.length, set.size(), "Set size does not match: "+ StringUtils.join(expected, ",")+" vs "+StringUtils.join(set, ","));
        for (String string : expected) {
            org.junit.jupiter.api.Assertions.assertTrue(set.contains(string), "Missing expected element " + string);
        }
        for (String found : set) {
            org.junit.jupiter.api.Assertions.assertTrue(Arrays.asList(expected).contains(found), "Unexpected element in set: " + found);
        }
    }

    public static void assertArraysEqual(String[] expected, String[] array) {
        org.junit.jupiter.api.Assertions.assertEquals(expected.length, array.length, "Set size does not match");

        for (int i=0; i<expected.length; i++) {
            org.junit.jupiter.api.Assertions.assertEquals("Difference in element "+i, expected[i], array[i]);
        }
    }

    public static void assertListsEqual(Object[] expected, List list, AssertFunction assertFunction) {
        org.junit.jupiter.api.Assertions.assertEquals(expected.length, list.size(), "List size does not match");

        for (int i=0; i<expected.length; i++) {
            assertFunction.check("Difference in element "+i, expected[i], list.get(i));
        }
    }

    public abstract static class AssertFunction {
        public abstract void check(String message, Object expected, Object actual);
    }
}
