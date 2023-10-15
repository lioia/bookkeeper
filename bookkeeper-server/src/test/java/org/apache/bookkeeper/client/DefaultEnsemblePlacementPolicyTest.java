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
    abstract static class SharedSetUpAndTearDown {
        protected DefaultEnsemblePlacementPolicy policy;

        @Before
        public void setUp() {
            ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
            conf.setDiskWeightBasedPlacementEnabled(true);
            policy = (DefaultEnsemblePlacementPolicy) new DefaultEnsemblePlacementPolicy()
                    .initialize(conf, Optional.empty(), null, DISABLE_ALL, NullStatsLogger.INSTANCE, null);
            Set<BookieId> validBookies = new HashSet<>();
            validBookies.add(BookieId.parse("validId"));
            policy.onClusterChanged(validBookies, new HashSet<>());
        }

        @After
        public void tearDown() {
            policy.uninitalize();
        }
    }

    @RunWith(Parameterized.class)
    public static class NewEnsembleMetadataTest extends SharedSetUpAndTearDown {
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
            ExpectedResult<PlacementResult<List<BookieId>>> fail =
                    new ExpectedResult<>(PlacementResult.of(new ArrayList<>(), PlacementPolicyAdherence.FAIL), null);
            ExpectedResult<PlacementResult<List<BookieId>>> valid =
                    new ExpectedResult<>(PlacementResult.of(Collections.singletonList(BookieId.parse("validId")),
                            PlacementPolicyAdherence.MEETS_STRICT), null);
            ExpectedResult<PlacementResult<List<BookieId>>> exception = new ExpectedResult<>(null, Exception.class);
            // ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies, Expected
            return Arrays.asList(
                    new Object[][]{
                            {-1, -2, -3, null, null, exception},
                            {-1, -2, -2, emptyMap, emptyList, exception},
                            {-1, -2, -1, validMap, validList, exception},
                            {-1, -1, -2, null, invalidList, exception},
                            {-1, -1, -1, emptyMap, mixList, exception},
                            {-1, -1, 0, validMap, null, exception},
                            {-1, 0, -1, null, emptyList, exception},
                            {-1, 0, 0, emptyMap, validList, exception},
                            {-1, 0, 1, validMap, invalidList, exception},
                            {0, -1, -2, null, mixList, fail},
                            {0, -1, -1, emptyMap, null, fail},
                            {0, -1, 0, validMap, emptyList, fail},
                            {0, 0, -1, null, validList, fail},
                            {0, 0, 0, emptyMap, invalidList, fail},
                            {0, 0, 1, validMap, mixList, fail},
                            {0, 1, 0, null, null, fail},
                            {0, 1, 1, emptyMap, emptyList, fail},
                            {0, 1, 2, validMap, validList, fail},
                            {1, 0, -1, null, invalidList, valid},
                            {1, 0, 0, emptyMap, mixList, exception},
                            {1, 0, 1, validMap, null, exception},
                            {1, 1, 0, null, emptyList, valid},
                            {1, 1, 1, emptyMap, validList, exception},
                            {1, 1, 2, validMap, invalidList, exception},
                            {1, 2, 1, null, mixList, exception},
                            {1, 2, 2, emptyMap, null, exception},
                            {1, 2, 3, validMap, emptyList, exception}
                    }
            );
        }

        private final int ensembleSize, writeQuorumSize, ackQuorumSize;
        private final Map<String, byte[]> customMetadata;
        private final Set<BookieId> excludeBookies;
        private final ExpectedResult<PlacementResult<List<BookieId>>> expected;

        public NewEnsembleMetadataTest(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                       Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies,
                                       ExpectedResult<PlacementResult<List<BookieId>>> expected) {
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.ackQuorumSize = ackQuorumSize;
            this.customMetadata = customMetadata;
            this.excludeBookies = excludeBookies;
            this.expected = expected;
        }

        @Test
        public void newEnsemble() {
            try {
                PlacementResult<List<BookieId>> result =
                        policy.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies);
                Assert.assertEquals(expected.getT().getAdheringToPolicy(), result.getAdheringToPolicy());
                Assert.assertEquals(expected.getT().getResult(), result.getResult());
            } catch (Exception ignored) {
                Assert.assertNotNull(expected.getException());
            }
        }
    }
}
