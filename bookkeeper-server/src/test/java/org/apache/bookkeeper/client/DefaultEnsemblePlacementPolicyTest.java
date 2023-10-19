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
                if (ensembleSize == 2 && excludeBookies.size() == 1) {
                    int i = 0;
                }
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
}
