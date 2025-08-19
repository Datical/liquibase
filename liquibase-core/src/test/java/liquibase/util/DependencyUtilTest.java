package liquibase.util;


import liquibase.util.DependencyUtil.DependencyGraph;
import liquibase.util.DependencyUtil.NodeValueListener;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;


public class DependencyUtilTest {

    private DependencyGraph<String> graph;
    private List<String> dependencyOrder = new ArrayList<String>();

    @BeforeEach
    public void setup() {
        dependencyOrder.clear();
        NodeValueListener<String> listener = new NodeValueListener<String>() {
            @Override public void evaluating(String nodeValue) {
                dependencyOrder.add(nodeValue);
            }
        };

        graph = new DependencyUtil.DependencyGraph<String>(listener);

    }

    @Test
    public void testBaseCase() {
        // a > b > c > d > e
        graph.add("a", "b");
        graph.add("b", "c");
        graph.add("c", "d");
        graph.add("d", "e");
        graph.computeDependencies();
        List<String> expected = Arrays.asList("a", "b", "c", "d", "e");
        Assertions.assertEquals(expected, dependencyOrder);
    }

    @Test
    public void testBranchingCase() {
        // a > b > c1
        //       > c2 > d
        graph.add("a", "b");
        graph.add("b", "c1");
        graph.add("b", "c2");
        graph.add("c2", "d");
        graph.computeDependencies();
        List<String> expected = Arrays.asList("a", "b", "c1", "c2", "d");
        Assertions.assertEquals(expected, dependencyOrder);
    }

    @Test
    public void testIndependentBranchesCase() {
        // a > b > c1
        //       > c2
        // o > p1 > r1 > s
        //     p2 > r2 > s2
        //             > s3
        // x > y
        graph.add("a", "b");
        graph.add("b", "c1");
        graph.add("b", "c2");
        graph.add("o", "p1");
        graph.add("p1", "r1");
        graph.add("r1", "s");
        graph.add("o", "p2");
        graph.add("p2", "r2");
        graph.add("r2", "s2");
        graph.add("r2", "s3");
        graph.add("x", "y");
        graph.computeDependencies();
        List<String> expected = Arrays.asList(
                "a", "o", "x",          // level 1
                "b", "p1", "p2", "y",   // level 2
                "c1", "c2", "r1", "r2", // level 3
                "s", "s2", "s3");       // roof
        Assertions.assertEquals(expected, dependencyOrder);
    }

    /* negative load */

    @Test
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    public void recursionSafetyCheck_edgeCase() {
        graph.add("a", "a");
        graph.computeDependencies();
        // assert test executes in less than 3 seconds
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    public void recursionSafetyCheck() {
        // a > B > c > d > B
        graph.add("a", "B");
        graph.add("B", "c");
        graph.add("c", "d");
        graph.add("d", "B");
        graph.computeDependencies();
        Assertions.assertTrue(dependencyOrder.contains("a")); // we try to capture something
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    public void recursionSafetyCheck2() {
        //     a > B > c > d
        // m > k > B
        // d > m (both have B in common)
        graph.add("a", "B");
        graph.add("B", "c");
        graph.add("c", "d");
        graph.add("m", "k");
        graph.add("k", "B");
        graph.add("d", "m");
        graph.computeDependencies();
        Assertions.assertTrue(dependencyOrder.contains("a"));
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    public void recursionSafetyCheck_rand() {
        Random rand = new Random();
        for (int i = 0; i < 100; i++) {
            graph.add(String.valueOf(rand.nextInt(50)), String.valueOf(rand.nextInt(50)));
        }
        graph.add("a", "b");
        graph.computeDependencies();
        Assertions.assertTrue(dependencyOrder.contains("a"));
        Assertions.assertTrue(dependencyOrder.contains("b"));
    }
}
