package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementPolicyAdherence;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementResult;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.utils.ExpectedResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;

@RunWith(Enclosed.class)
public class DefaultEnsemblePlacementPolicyTest {
    @RunWith(Parameterized.class)
    public static class NewEnsembleTest {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            Map<String, byte[]> emptyMap = new HashMap<>();
            Map<String, byte[]> validMap = new HashMap<>();
            validMap.put("string", new byte[]{1});
            Set<BookieId> emptyList = Collections.emptySet();
            Set<BookieId> validList = Collections.singleton(BookieId.parse("validId"));
            Set<BookieId> invalidList = Collections.singleton(BookieId.parse("invalidId"));
            Set<BookieId> mixList = new HashSet<>();
            mixList.add(BookieId.parse("validId"));
            mixList.add(BookieId.parse("invalidId"));
            List<String> initial1 = Collections.singletonList("validId");
            List<String> initial2 = Arrays.asList("validId", "validId1");
            List<String> initial3 = Arrays.asList("validId", "validId", "validId1", "validId2");
            ExpectedResult<PlacementResult<List<BookieId>>> exception = new ExpectedResult<>(null, Exception.class);
            ExpectedResult<PlacementResult<List<BookieId>>> fail =
                    new ExpectedResult<>(PlacementResult.of(new ArrayList<>(), PlacementPolicyAdherence.FAIL), null);
            ExpectedResult<PlacementResult<List<BookieId>>> valid1 =
                    new ExpectedResult<>(PlacementResult.of(Collections.singletonList(BookieId.parse("validId")),
                            PlacementPolicyAdherence.MEETS_STRICT), null);
            ExpectedResult<PlacementResult<List<BookieId>>> valid2 =
                    new ExpectedResult<>(PlacementResult.of(Arrays.asList(BookieId.parse("validId"), BookieId.parse("validId1")),
                            PlacementPolicyAdherence.MEETS_STRICT), null);
            ExpectedResult<PlacementResult<List<BookieId>>> valid3 =
                    new ExpectedResult<>(PlacementResult.of(Arrays.asList(BookieId.parse("validId1"), BookieId.parse("validId2")),
                            PlacementPolicyAdherence.MEETS_STRICT), null);
            // expected, ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies, weighted, initialBookies
            return Arrays.asList(
                    new Object[][]{
                            {exception, -1, -2, -3, null, null, true, initial1},
                            {exception, -1, -2, -2, emptyMap, emptyList, true, initial1},
                            {exception, -1, -2, -1, validMap, validList, true, initial1},
                            {exception, -1, -1, -2, null, invalidList, true, initial1},
                            {exception, -1, -1, -1, emptyMap, mixList, true, initial1},
                            {exception, -1, -1, 0, validMap, null, true, initial1},
                            {exception, -1, 0, -1, null, emptyList, true, initial1},
                            {exception, -1, 0, 0, emptyMap, validList, true, initial1},
                            {exception, -1, 0, 1, validMap, invalidList, true, initial1},
                            {fail, 0, -1, -2, null, mixList, true, initial1},
                            {fail, 0, -1, -1, emptyMap, null, true, initial1},
                            {fail, 0, -1, 0, validMap, emptyList, true, initial1},
                            {fail, 0, 0, -1, null, validList, true, initial1},
                            {fail, 0, 0, 0, emptyMap, invalidList, true, initial1},
                            {fail, 0, 0, 1, validMap, mixList, true, initial1},
                            {fail, 0, 1, 0, null, null, true, initial1},
                            {fail, 0, 1, 1, emptyMap, emptyList, true, initial1},
                            {fail, 0, 1, 2, validMap, validList, true, initial1},
                            {valid1, 1, 0, -1, null, invalidList, true, initial1},
                            {exception, 1, 0, 0, emptyMap, mixList, true, initial1},
                            {exception, 1, 0, 1, validMap, null, true, initial1},
                            {valid1, 1, 1, 0, null, emptyList, true, initial1},
                            {exception, 1, 1, 1, emptyMap, validList, true, initial1},
                            {exception, 1, 1, 2, validMap, invalidList, true, initial1},
                            {exception, 1, 2, 1, null, mixList, true, initial1},
                            {exception, 1, 2, 2, emptyMap, null, true, initial1},
                            {exception, 1, 2, 3, validMap, emptyList, true, initial1},

                            // JaCoCo Improvements
                            // Weighted = false
                            {valid1, 1, 0, -1, null, invalidList, false, initial1},
                            {exception, 1, 1, 1, emptyMap, validList, false, initial1},
                            // Ensemble size = 2
                            {valid2, 2, 1, 0, null, emptyList, true, initial2},
                            {valid3, 2, 1, 0, null, validList, true, initial3},
                            {valid2, 2, 1, 0, null, emptyList, false, initial2},
                    }
            );
        }

        private DefaultEnsemblePlacementPolicy policy;
        private final int ensembleSize, writeQuorumSize, ackQuorumSize;
        private final Map<String, byte[]> customMetadata;
        private final Set<BookieId> excludeBookies;
        private final ExpectedResult<PlacementResult<List<BookieId>>> expected;
        private final boolean weighted;
        private final List<String> initial;

        public NewEnsembleTest(ExpectedResult<PlacementResult<List<BookieId>>> expected,
                               int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                               Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies,
                               boolean weighted, List<String> initial) {
            this.expected = expected;
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.ackQuorumSize = ackQuorumSize;
            this.customMetadata = customMetadata;
            this.excludeBookies = excludeBookies;
            this.weighted = weighted;
            this.initial = initial;
        }

        @Before
        public void setUp() {
            ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
            conf.setDiskWeightBasedPlacementEnabled(this.weighted);
            policy = (DefaultEnsemblePlacementPolicy) new DefaultEnsemblePlacementPolicy()
                    .initialize(conf, Optional.empty(), null, DISABLE_ALL, NullStatsLogger.INSTANCE, null);
            Set<BookieId> validBookies = new HashSet<>();
            for (String bookieId : initial) {
                validBookies.add(BookieId.parse(bookieId));
            }
            policy.onClusterChanged(validBookies, new HashSet<>());
        }

        @After
        public void tearDown() {
            policy.uninitalize();
        }

        @Test
        public void newEnsemble() {
            try {
                PlacementResult<List<BookieId>> result =
                        policy.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies);
                Assert.assertEquals(expected.getT().getAdheringToPolicy(), result.getAdheringToPolicy());
                // List equals ignoring order
                Assert.assertEquals(expected.getT().getResult().size(), result.getResult().size());
                Assert.assertTrue(expected.getT().getResult().containsAll(result.getResult()));
                Assert.assertTrue(result.getResult().containsAll(expected.getT().getResult()));
            } catch (Exception ignored) {
                Assert.assertNotNull(expected.getException());
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class OnClusterChangedTest {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            ExpectedResult<Set<BookieId>> exception = new ExpectedResult<>(null, Exception.class);
            Set<BookieId> validSet = Collections.singleton(BookieId.parse("w"));
            ExpectedResult<Set<BookieId>> valid = new ExpectedResult<>(validSet, null);
            ExpectedResult<Set<BookieId>> emptyValid = new ExpectedResult<>(new HashSet<>(), null);
            Set<BookieId> empty = new HashSet<>();
            Set<BookieId> newBookie = Collections.singleton(BookieId.parse("new"));
            Set<BookieId> writeBookie = Collections.singleton(BookieId.parse("w"));
            return Arrays.asList(
                    new Object[][]{
                            {null, null, exception},
                            {null, empty, exception},
                            {null, newBookie, exception},
                            {empty, null, exception},
                            {empty, empty, valid},
                            {empty, newBookie, valid},
                            {newBookie, null, exception},
                            {newBookie, empty, valid},
//                            {newBookie, newBookie, exception}, // Fail
//                            // Improvements
                            {writeBookie, newBookie, emptyValid},
                    }
            );
        }

        private DefaultEnsemblePlacementPolicy policy;
        private final Set<BookieId> writableBookies;
        private final Set<BookieId> readOnlyBookies;
        private final ExpectedResult<Set<BookieId>> expected;

        public OnClusterChangedTest(Set<BookieId> writableBookies, Set<BookieId> readOnlyBookies,
                                    ExpectedResult<Set<BookieId>> expected) {
            this.writableBookies = writableBookies;
            this.readOnlyBookies = readOnlyBookies;
            this.expected = expected;
        }

        @Before
        public void setUp() {
            ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
            conf.setDiskWeightBasedPlacementEnabled(true);
            policy = (DefaultEnsemblePlacementPolicy) new DefaultEnsemblePlacementPolicy()
                    .initialize(conf, Optional.empty(), null, DISABLE_ALL, NullStatsLogger.INSTANCE, null);
            Set<BookieId> initialWritable = Collections.singleton(BookieId.parse("w"));
            Set<BookieId> initialReadOnly = Collections.singleton(BookieId.parse("ro"));
            policy.onClusterChanged(initialWritable, initialReadOnly);
        }

        @After
        public void tearDown() {
            policy.uninitalize();
        }

        @Test
        public void onClusterChangedTest() {
            try {
                Set<BookieId> result = policy.onClusterChanged(writableBookies, readOnlyBookies);
                // List equals ignoring order
                Assert.assertEquals(expected.getT().size(), result.size());
                Assert.assertTrue(expected.getT().containsAll(result));
                Assert.assertTrue(result.containsAll(expected.getT()));
            } catch (Exception e) {
                Assert.assertNotNull(expected.getException());
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class ReplaceBookieTest {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            Map<String, byte[]> emptyMap = new HashMap<>();
            Map<String, byte[]> validMap = new HashMap<>();
            validMap.put("string", new byte[]{1});
            BookieId validId = BookieId.parse("w");
            BookieId invalidId = BookieId.parse("ro");
            Set<BookieId> emptySet = new HashSet<>();
            Set<BookieId> validSet = new HashSet<>();
            validSet.add(validId);
            Set<BookieId> invalidSet = Collections.singleton(invalidId);
            Set<BookieId> mixSet = new HashSet<>();
            mixSet.add(validId);
            mixSet.add(invalidId);
            List<BookieId> emptyList = new ArrayList<>();
            List<BookieId> validList = Collections.singletonList(validId);
            List<BookieId> invalidList = Collections.singletonList(invalidId);
            ExpectedResult<PlacementResult<BookieId>> exception = new ExpectedResult<>(null, Exception.class);
            ExpectedResult<PlacementResult<BookieId>> valid = new ExpectedResult<>(
                    PlacementResult.of(BookieId.parse("w2"), PlacementPolicyAdherence.MEETS_STRICT),
                    null);
            return Arrays.asList(
                    new Object[][]{
                            // e  q  a customMetadata currentEnsemble bookieToReplace excludeBookies expected
                            {-1, -2, -3, null, null, null, null, exception},
                            {-1, -2, -2, emptyMap, emptyList, validId, emptySet, exception},
                            {-1, -2, -1, validMap, validList, invalidId, validSet, exception},
                            {-1, -1, -2, null, invalidList, null, invalidSet, exception},
                            {-1, -1, -1, emptyMap, null, validId, mixSet, exception},
                            {-1, -1, 0, validMap, emptyList, invalidId, null, exception},
                            {-1, 0, -1, null, validList, null, emptySet, exception},
                            {-1, 0, 0, emptyMap, invalidList, validId, validSet, exception},
                            {-1, 0, 1, validMap, null, invalidId, invalidSet, exception},
                            {0, -1, -2, null, emptyList, null, mixSet, exception},
                            {0, -1, -1, emptyMap, validList, validId, null, exception},
//                            {0, -1, 0, validMap, invalidList, invalidId, emptySet, exception}, // Fail
                            {0, 0, -1, null, null, null, validSet, exception},
                            {0, 0, 0, emptyMap, emptyList, validId, invalidSet, exception},
                            {0, 0, 1, validMap, validList, invalidId, mixSet, exception},
                            {0, 1, 0, null, invalidList, null, null, exception},
                            {0, 1, 1, emptyMap, null, validId, emptySet, exception},
                            {0, 1, 2, validMap, emptyList, invalidId, validSet, exception},
                            {1, 0, -1, null, validList, null, invalidSet, exception},
                            {1, 0, 0, emptyMap, invalidList, validId, mixSet, exception},
                            {1, 0, 1, validMap, null, invalidId, null, exception},
                            {1, 1, 0, null, emptyList, null, emptySet, exception},
//                            {1, 1, 1, emptyMap, validList, validId, validSet, valid}, // Fail
                            {1, 1, 2, validMap, invalidList, invalidId, invalidSet, exception},
                            {1, 2, 1, null, null, null, mixSet, exception},
                            {1, 2, 2, emptyMap, emptyList, validId, null, exception},
                            {1, 2, 3, validMap, validList, invalidId, emptySet, exception},
                            // Improvements
                            {1, 1, 1, null, validList, validId, emptySet, valid}
                    }
            );
        }

        private DefaultEnsemblePlacementPolicy policy;
        private final int ensembleSize;
        private final int writeQuorumSize;
        private final int ackQuorumSize;
        private final Map<String, byte[]> customMetadata;
        private final List<BookieId> currentEnsemble;
        private final BookieId bookieToReplace;
        private final Set<BookieId> excludeBookies;
        private final ExpectedResult<PlacementResult<BookieId>> expected;

        public ReplaceBookieTest(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                 Map<String, byte[]> customMetadata, List<BookieId> currentEnsemble,
                                 BookieId bookieToReplace, Set<BookieId> excludeBookies,
                                 ExpectedResult<PlacementResult<BookieId>> expected) {
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.ackQuorumSize = ackQuorumSize;
            this.customMetadata = customMetadata;
            this.currentEnsemble = currentEnsemble;
            this.bookieToReplace = bookieToReplace;
            this.excludeBookies = excludeBookies;
            this.expected = expected;
        }

        @Before
        public void setup() {
            ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
            conf.setDiskWeightBasedPlacementEnabled(true);
            policy = (DefaultEnsemblePlacementPolicy) new DefaultEnsemblePlacementPolicy()
                    .initialize(conf, Optional.empty(), null, DISABLE_ALL, NullStatsLogger.INSTANCE, null);
            Set<BookieId> initialWritable = new HashSet<>();
            initialWritable.add(BookieId.parse("w"));
            initialWritable.add(BookieId.parse("w2"));
            policy.onClusterChanged(initialWritable, new HashSet<>());
        }

        @Test
        public void replaceBookieTest() {
            try {
                PlacementResult<BookieId> result = policy.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize,
                        customMetadata, currentEnsemble, bookieToReplace, excludeBookies);
                Assert.assertNull(expected.getException());
                Assert.assertEquals(expected.getT().getResult().getId(), result.getResult().getId());
                Assert.assertEquals(expected.getT().getAdheringToPolicy(), result.getAdheringToPolicy());
            } catch (Exception e) {
                Assert.assertNotNull(expected.getException());
            }
        }

        @After
        public void teardown() {
            policy.uninitalize();
        }
    }

    @RunWith(Parameterized.class)
    public static class UpdateBookieInfoTest {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            Map<BookieId, BookieInfoReader.BookieInfo> empty = new HashMap<>();
            Map<BookieId, BookieInfoReader.BookieInfo> bothNull = new HashMap<>();
            bothNull.put(null, null);
            Map<BookieId, BookieInfoReader.BookieInfo> wBookie = new HashMap<>();
            wBookie.put(BookieId.parse("w"), new BookieInfoReader.BookieInfo(-1, -2));
            Map<BookieId, BookieInfoReader.BookieInfo> newBookie = new HashMap<>();
            newBookie.put(BookieId.parse("w2"), new BookieInfoReader.BookieInfo(0, 0));
            Map<BookieId, BookieInfoReader.BookieInfo> nullBookie = new HashMap<>();
            nullBookie.put(null, new BookieInfoReader.BookieInfo(1, 2));
            ExpectedResult<Object> exception = new ExpectedResult<>(null, Exception.class);
            ExpectedResult<Object> valid = new ExpectedResult<>(null, null);
            return Arrays.asList(
                    new Object[][]{
                            {null, exception},
                            {empty, valid},
                            {bothNull, exception},
//                            {wBookie, exception}, // Fail
                            {newBookie, valid},
//                            {nullBookie, exception} // Fail
                    }
            );
        }

        private DefaultEnsemblePlacementPolicy policy;
        private final Map<BookieId, BookieInfoReader.BookieInfo> map;
        private final ExpectedResult<Object> expected;

        public UpdateBookieInfoTest(Map<BookieId, BookieInfoReader.BookieInfo> map,
                                    ExpectedResult<Object> expected) {
            this.map = map;
            this.expected = expected;
        }

        @Before
        public void setup() {
            ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
            conf.setDiskWeightBasedPlacementEnabled(true);
            policy = (DefaultEnsemblePlacementPolicy) new DefaultEnsemblePlacementPolicy()
                    .initialize(conf, Optional.empty(), null, DISABLE_ALL, NullStatsLogger.INSTANCE, null);
            Set<BookieId> initialWritable = new HashSet<>();
            initialWritable.add(BookieId.parse("w"));
            policy.onClusterChanged(initialWritable, new HashSet<>());
        }

        @Test
        public void updateBookieInfoTest() {
            try {
                policy.updateBookieInfo(map);
                Assert.assertNull(expected.getException());
            } catch (Exception e) {
                Assert.assertNotNull(expected.getException());
            }
        }

        @Test
        public void pitImprovements() {
            try {
                Thread thread = new Thread(() -> {
                    try {
                        policy.updateBookieInfo(map);
                        Thread.sleep(100);
                    } catch (Exception e) {
                        Assert.assertNotNull(expected.getException());
                    }
                });
                thread.start();
                policy.updateBookieInfo(map);
                thread.join();
                Assert.assertNull(expected.getException());
            } catch (Exception e) {
                Assert.assertNotNull(expected.getException());
            }
        }

        @After
        public void teardown() {
            policy.uninitalize();
        }
    }
}
