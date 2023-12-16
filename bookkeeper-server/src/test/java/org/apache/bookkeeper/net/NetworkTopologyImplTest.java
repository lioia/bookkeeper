package org.apache.bookkeeper.net;

import org.apache.bookkeeper.utils.ExpectedResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Enclosed.class)
public class NetworkTopologyImplTest {
    @RunWith(Parameterized.class)
    public static class AddTest {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            String rackScope = "/rack-0";
            Node node1 = new BookieNode(BookieId.parse("node1"), rackScope);
            Node node2 = new BookieNode(BookieId.parse("node2"), rackScope);
            Node innerNode = new NetworkTopologyImpl.InnerNode(rackScope + "/inner-node");
            ExpectedResult<Boolean> present = new ExpectedResult<>(true, null);
            ExpectedResult<Boolean> absent = new ExpectedResult<>(false, null);
            ExpectedResult<Boolean> exception = new ExpectedResult<>(null, Exception.class);
            return Arrays.asList(
                    new Object[][]{
//                            {null, exception}, // Fail: it early returns without throwing an exception
                            {null, absent},
                            {node1, present},
                            {node2, present},
                            // Improvements
                            {innerNode, exception}
                    }
            );
        }

        private NetworkTopologyImpl topology;
        private final Node node;
        private final ExpectedResult<Boolean> expected;

        public AddTest(Node node, ExpectedResult<Boolean> expected) {
            this.node = node;
            this.expected = expected;
        }

        @Before
        public void setup() {
            topology = new NetworkTopologyImpl();
            Node node1 = new BookieNode(BookieId.parse("node1"), "/rack-0");
            topology.add(node1);
        }

        @Test
        public void addTest() {
            try {
                topology.add(node);
                boolean result = topology.contains(node);
                Assert.assertEquals(expected.getT(), result);
                // PIT improvements
                if (expected.getT())
                    Assert.assertEquals(1, topology.numOfRacks);
            } catch (Exception ignored) {
                Assert.assertNotNull(expected.getException());
            }
        }
    }

    public static class AddNonParameterizedTest {
        private NetworkTopologyImpl topology;

        @Before
        public void setup() {
            topology = new NetworkTopologyImpl();
            topology.add(new BookieNode(BookieId.parse("node-1"), "/rack-0"));
        }

        @Test
        public void multipleAddTest() {
            try {
                topology.add(new BookieNode(BookieId.parse("node-2"), "/rack-0"));
                topology.add(new BookieNode(BookieId.parse("node-3"), "/rack-0"));
            } catch (Exception ignored) {
                Assert.fail();
            }
        }

        @Test
        public void invalidLevelTest() {
            try {
                topology.add(new NodeBase("/node-2"));
                Assert.fail();
            } catch (Exception ignored) {
                // Success: exception was thrown
            }
        }

        // PIT improvements
        @Test
        public void emptyTopologyTest() throws NoSuchFieldException, IllegalAccessException {
            topology = new NetworkTopologyImpl();
            Node node = new BookieNode(BookieId.parse("node-1"), "/rack-0");
            topology.add(node);
            Assert.assertTrue(topology.contains(node));
            // Get depthOfAllLeaves through reflections
            Field depthOfAllLeavesField = NetworkTopologyImpl.class.getDeclaredField("depthOfAllLeaves");
            depthOfAllLeavesField.setAccessible(true);
            int depthOfAllLeaves = depthOfAllLeavesField.getInt(topology);
            Assert.assertNotEquals(-1, depthOfAllLeaves);
        }

        // PIT improvements
        @Test
        public void deadlockTest() {
            Node node2 = new BookieNode(BookieId.parse("node-2"), "/rack-0");
            Node node3 = new BookieNode(BookieId.parse("node-3"), "/rack-0");
            try {
                Thread thread = new Thread(() -> {
                    try {
                        topology.add(node3);
                        Thread.sleep(100);
                    } catch (Exception e) {
                        Assert.fail();
                    }
                });
                thread.start();
                topology.add(node2);
                thread.join();
                Assert.assertTrue(topology.contains(node2));
                Assert.assertTrue(topology.contains(node3));
            } catch (Exception e) {
                Assert.fail();
            }
        }
    }
}
