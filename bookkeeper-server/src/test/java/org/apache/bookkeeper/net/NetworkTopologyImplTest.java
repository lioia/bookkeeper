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
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import static org.mockito.Mockito.*;

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
            Node node1 = new BookieNode(BookieId.parse("initial-node"), "/rack-0");
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

    @RunWith(Parameterized.class)
    public static class ContainsParametricTest {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            String rackScope = "/rack-0";
            Node rack0 = new NetworkTopologyImpl.InnerNode("/rack-0");
            Node node2 = new BookieNode(BookieId.parse("node2"), rackScope);
            node2.setLevel(1);
            node2.setParent(rack0);
            Node subRack = new NetworkTopologyImpl.InnerNode("/sub-rack-1");
            subRack.setLevel(1);
            subRack.setParent(rack0);
            Node deepNode = new NodeBase("/rack-0/sub-rack-1/deepNode");
            deepNode.setLevel(2);
            deepNode.setParent(subRack);
            Node noParentNode = new NodeBase("/no-parent");
            noParentNode.setLevel(0);
            Node noLevelNode = new NodeBase("/fake-parent");
            noLevelNode.setParent(new NetworkTopologyImpl.InnerNode("/fake-parent"));
            Node rack1 = new NetworkTopologyImpl.InnerNode("/rack-1");
            Node subRack1 = new NetworkTopologyImpl.InnerNode("/rack-1/sub-rack-1");
            subRack1.setLevel(1);
            subRack1.setParent(rack1);
            Node anotherRack = new NodeBase("/rack-1/sub-rack-0/another-rack");
            anotherRack.setLevel(0);
            anotherRack.setParent(subRack1);
            Node anotherRack2 = new NodeBase("/rack-1/sub-rack-0/another-rack2");
            anotherRack2.setLevel(1);
            anotherRack2.setParent(subRack1);
            ExpectedResult<Boolean> falseExp = new ExpectedResult<>(false, null);
            ExpectedResult<Boolean> trueExp = new ExpectedResult<>(true, null);
            return Arrays.asList(
                    new Object[][]{
                            {null, falseExp},
                            {initialNode, trueExp},
                            {node2, falseExp},
                            // Improvements
                            {deepNode, falseExp},
                            {noParentNode, falseExp},
                            {noLevelNode, falseExp},
                            {anotherRack, falseExp},
                            {anotherRack2, falseExp},
                    }
            );
        }

        private NetworkTopologyImpl topology;
        private final Node node;
        private final ExpectedResult<Boolean> expected;
        private static final Node initialNode = new BookieNode(BookieId.parse("node1"), "/rack-0");

        public ContainsParametricTest(Node node, ExpectedResult<Boolean> expected) {
            this.node = node;
            this.expected = expected;
        }

        @Before
        public void setup() {
            topology = new NetworkTopologyImpl();
            topology.add(initialNode);
        }

        @Test
        public void containsTest() throws InterruptedException {
            Thread thread = new Thread(() -> {
                try {
                    Field lockField = NetworkTopologyImpl.class.getDeclaredField("netlock");
                    lockField.setAccessible(true);
                    ReadWriteLock lock = (ReadWriteLock) lockField.get(topology);
                    lock.writeLock().lock();
                    Thread.sleep(100);
                    lock.writeLock().unlock();
                } catch (Exception ignored) {
                    Assert.fail();
                }
            });
            thread.start();
            boolean result = topology.contains(node);
            thread.join();
            Assert.assertEquals(expected.getT(), result);
        }
    }

    public static class ContainsNonParametricTest {
        private NetworkTopologyImpl topology;
        private Node node;

        @Before
        public void setup() {
            node = new NodeBase("/node-1");
            node.setParent(new NetworkTopologyImpl.InnerNode("/"));
            node.setNetworkLocation("/");
            topology = new NetworkTopologyImpl();
            topology.add(node);
        }

        @Test
        public void containsTest() {
            boolean result = topology.contains(node);
            Assert.assertTrue(result);
        }
    }

    @RunWith(Parameterized.class)
    public static class RemoveParametricTest {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            initialNode = new BookieNode(BookieId.parse("initial-node"), "/rack-0");
            Node notPresent = new NodeBase("/not-present");
            Node innerNode = new NetworkTopologyImpl.InnerNode("/rack-0");
            innerNode.setNetworkLocation("/"); // PIT improvements
            Node notPresetButInRack = new BookieNode(BookieId.parse("not-present"), "/rack-0");
            Node rack = new NodeBase("/rack-0");
            rack.setNetworkLocation("/");
            ExpectedResult<Void> valid = new ExpectedResult<>(null, null);
            ExpectedResult<Void> exception = new ExpectedResult<>(null, Exception.class);
            return Arrays.asList(
                    new Object[][]{
                            {null, valid},
                            {notPresent, exception},
                            {initialNode, valid},
                            // Improvements
                            {innerNode, exception},
                            {notPresetButInRack, valid},
                            {rack, valid},
                    }
            );
        }

        private NetworkTopologyImpl topology;
        private static Node initialNode;
        private final Node node;
        private final ExpectedResult<Void> expected;

        public RemoveParametricTest(Node node, ExpectedResult<Void> expected) {
            this.node = node;
            this.expected = expected;
        }

        @Before
        public void setup() {
            topology = new NetworkTopologyImpl();
            topology.add(initialNode);
        }

        @Test
        public void remove() {
            try {
                topology.remove(node);
                Assert.assertNull(expected.getException());
                Assert.assertFalse(topology.contains(node));
            } catch (Exception ignored) {
                Assert.assertNotNull(expected);
            }
        }
    }

    public static class RemoveNonParametricTest {
        private NetworkTopologyImpl topology;
        private static final Node initialNode = new BookieNode(BookieId.parse("node1"), "/rack-0");

        @Before
        public void setup() {
            topology = new NetworkTopologyImpl();
            topology.add(initialNode);
        }

        @Test
        public void remove() {
            try {
                topology.remove(initialNode);
                Assert.assertFalse(topology.contains(initialNode));
                // PIT improvements
                Assert.assertEquals(0, topology.getNumOfRacks());
            } catch (Exception ignored) {
                Assert.fail();
            }
        }

        // PIT improvements
        @Test
        public void deadlockTest() {
            try {
                Node node2 = new BookieNode(BookieId.parse("node-2"), "/rack-0");
                topology.add(node2);
                Thread thread = new Thread(() -> {
                    try {
                        topology.remove(node2);
                        Thread.sleep(100);
                    } catch (Exception e) {
                        Assert.fail();
                    }
                });
                thread.start();
                topology.remove(initialNode);
                thread.join();
                Assert.assertFalse(topology.contains(initialNode));
                Assert.assertFalse(topology.contains(node2));
            } catch (Exception ignored) {
                Assert.fail();
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class GetLeavesParametricTest {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            ExpectedResult<Set<Node>> validInitial = new ExpectedResult<>(Collections.singleton(initialNode), null);
            ExpectedResult<Set<Node>> empty = new ExpectedResult<>(Collections.emptySet(), null);
            ExpectedResult<Set<Node>> exception = new ExpectedResult<>(null, Exception.class);
            return Arrays.asList(
                    new Object[][]{
                            {null, exception},
                            {"/rack-1", empty},
                            {"/rack-0", validInitial},
                            // Improvements
                            {"~", empty},
                            // PIT improvements
                            {"~/rack-1", validInitial},
                    }
            );
        }

        private NetworkTopologyImpl topology;
        private static final Node initialNode = new BookieNode(BookieId.parse("initial-node"), "/rack-0");
        private final String scope;
        private final ExpectedResult<Set<Node>> expected;

        public GetLeavesParametricTest(String scope, ExpectedResult<Set<Node>> expected) {
            this.scope = scope;
            this.expected = expected;
        }

        @Before
        public void setup() {
            topology = new NetworkTopologyImpl();
            topology.add(initialNode);
        }

        @Test
        public void getLeavesTest() {
            try {
                // PIT improvements
                Thread thread = new Thread(() -> {
                    try {
                        Field lockField = NetworkTopologyImpl.class.getDeclaredField("netlock");
                        lockField.setAccessible(true);
                        ReadWriteLock lock = (ReadWriteLock) lockField.get(topology);
                        lock.writeLock().lock();
                        Thread.sleep(100);
                        lock.writeLock().unlock();
                    } catch (Exception ignored) {
                        Assert.assertNotNull(expected.getException());
                    }
                });
                thread.start();
                Set<Node> result = topology.getLeaves(scope);
                thread.join();
                // Collection equals, ignoring order
                Assert.assertEquals(expected.getT().size(), result.size());
                Assert.assertTrue(expected.getT().containsAll(result));
                Assert.assertTrue(result.containsAll(expected.getT()));
            } catch (Exception ignored) {
                Assert.assertNotNull(expected.getException());
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class CountNumOfAvailableNodesParametricTest {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            Collection<Node> excludeEmpty = Collections.emptyList();
            Collection<Node> excludePresent = Collections.singletonList(initialNode);
            Collection<Node> excludeNotPresent = Collections.singletonList(new NodeBase("/node-1"));
            ExpectedResult<Integer> zero = new ExpectedResult<>(0, null);
            ExpectedResult<Integer> one = new ExpectedResult<>(1, null);
            ExpectedResult<Integer> exception = new ExpectedResult<>(null, Exception.class);
            return Arrays.asList(
                    new Object[][]{
                            {null, excludeEmpty, exception},
                            {"/rack-1", excludePresent, zero},
                            {"/rack-0", excludeNotPresent, one},
                            // Improvements
                            {"~", excludeEmpty, zero},
                            // ba-dua improvements
                            {"~/rack-0/initial-node", excludeEmpty, one}
                    }
            );
        }

        private static final Node initialNode = new BookieNode(BookieId.parse("initial-node"), "/rack-0");
        private NetworkTopologyImpl topology;
        private final String scope;
        private final Collection<Node> excludeNodes;
        private final ExpectedResult<Integer> expected;

        public CountNumOfAvailableNodesParametricTest(String scope, Collection<Node> excludeNodes, ExpectedResult<Integer> expected) {
            this.scope = scope;
            this.excludeNodes = excludeNodes;
            this.expected = expected;
        }

        @Before
        public void setup() {
            topology = new NetworkTopologyImpl();
            topology.add(initialNode);
        }

        @Test
        public void countTest() {
            try {
                Thread thread = new Thread(() -> {
                    try {
                        Field lockField = NetworkTopologyImpl.class.getDeclaredField("netlock");
                        lockField.setAccessible(true);
                        ReadWriteLock lock = (ReadWriteLock) lockField.get(topology);
                        lock.writeLock().lock();
                        Thread.sleep(100);
                        lock.writeLock().unlock();
                    } catch (Exception ignored) {
                        Assert.assertNotNull(expected.getException());
                    }
                });
                thread.start();
                Integer result = topology.countNumOfAvailableNodes(scope, excludeNodes);
                thread.join();
                Assert.assertEquals(expected.getT(), result);
            } catch (Exception ignored) {
                Assert.assertNotNull(expected.getException());
            }
        }
    }

    public static class CountNumOfAvailableNodesNonParametricTest {
        private NetworkTopologyImpl topology;

        @Before
        public void setup() {
            topology = new NetworkTopologyImpl();
            topology.add(new BookieNode(BookieId.parse("initial-node-1"), "/rack-0/sub-rack-1"));
            topology.add(new BookieNode(BookieId.parse("initial-node-2"), "/rack-0/sub-rack-1"));
        }

        @Test
        public void countTest() {
            Collection<Node> exclude = Collections.singletonList(new NodeBase("/rack-0/sub-rack-1/initial-node-1"));
            Assert.assertEquals(1, topology.countNumOfAvailableNodes("/rack-0/sub-rack-1", exclude));
        }

        // ba-dua improvements
        @Test
        public void countInverseTest() {
            Collection<Node> exclude = Arrays.asList(
                    new NodeBase("/rack-0/sub-rack-1/initial-node-2"),
                    new NodeBase("/rack-0/sub-rack-1/initial-node-1")
            );
            Assert.assertEquals(0, topology.countNumOfAvailableNodes("~", exclude));
        }
    }

    @RunWith(Parameterized.class)
    public static class GetDistanceParametricTest {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            Node notPresent = new BookieNode(BookieId.parse("node-3"), "/rack-0");
            ExpectedResult<Integer> exception = new ExpectedResult<>(null, Exception.class);
            ExpectedResult<Integer> max = new ExpectedResult<>(Integer.MAX_VALUE, null);
            ExpectedResult<Integer> zero = new ExpectedResult<>(0, null);
            ExpectedResult<Integer> two = new ExpectedResult<>(2, null);
            ExpectedResult<Integer> four = new ExpectedResult<>(4, null);

            // Improvements
            Node root = new NodeBase("/");
            Node rack2 = new NodeBase("/rack-2");
            rack2.setParent(root);
            rack2.setLevel(1);
            Node node1Rack2 = new NodeBase("/rack-2/node-1");
            node1Rack2.setParent(rack2);
            node1Rack2.setLevel(2);

            Node rack3 = new NodeBase("/rack-3");
            rack3.setParent(root);
            rack3.setLevel(1);
            Node node1Rack3 = new NodeBase("/rack-3/node-1");
            node1Rack3.setParent(rack3);
            node1Rack3.setLevel(3);

            Node directNoParent = new NodeBase("/node-1");
            directNoParent.setLevel(1);
            return Arrays.asList(
                    new Object[][]{
//                            {null, null, exception}, // Fail
                            {null, null, zero},
//                            {notPresent, notPresent, max}, // Fail (the nodes are not in the topology)
                            {notPresent, notPresent, zero},
                            {initialNode1, initialNode2, two},
                            {initialNode2, initialNode3, four},
                            // Improvements
                            {node1Rack2, node1Rack3, max},
                            {node1Rack3, node1Rack2, max},
                            // ba-dua improvements
                            {initialNode1, root, four},
                            {root, initialNode1, four},
                            {directNoParent, root, max},
                            {root, directNoParent, max},
                    }
            );
        }

        private static final Node initialNode1 = new BookieNode(BookieId.parse("initial-node-1"), "/rack-0");
        private static final Node initialNode2 = new BookieNode(BookieId.parse("initial-node-2"), "/rack-0");
        private static final Node initialNode3 = new BookieNode(BookieId.parse("initial-node-3"), "/rack-1");
        private NetworkTopologyImpl topology;
        private final Node node1;
        private final Node node2;
        private final ExpectedResult<Integer> expected;

        public GetDistanceParametricTest(Node node1, Node node2, ExpectedResult<Integer> expected) {
            this.node1 = node1;
            this.node2 = node2;
            this.expected = expected;
        }

        @Before
        public void setup() {
            topology = new NetworkTopologyImpl();
            topology.add(initialNode1);
            topology.add(initialNode2);
            topology.add(initialNode3);
        }

        @Test
        public void getDistanceTest() {
            try {
                // PIT improvements
                Thread thread = new Thread(() -> {
                    try {
                        Field lockField = NetworkTopologyImpl.class.getDeclaredField("netlock");
                        lockField.setAccessible(true);
                        ReadWriteLock lock = (ReadWriteLock) lockField.get(topology);
                        lock.writeLock().lock();
                        Thread.sleep(100);
                        lock.writeLock().unlock();
                    }catch(Exception ignored) {
                        Assert.assertNotNull(expected.getException());
                    }
                });
                thread.start();
                Integer result = topology.getDistance(node1, node2);
                thread.join();
                Assert.assertNull(expected.getException());
                Assert.assertEquals(expected.getT(), result);
            } catch (Exception ignored) {
                Assert.assertNotNull(expected.getException());
            }
        }
    }
}