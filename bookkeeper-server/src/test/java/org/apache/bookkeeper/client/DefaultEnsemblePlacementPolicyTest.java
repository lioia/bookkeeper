package org.apache.bookkeeper.client;

import org.apache.bookkeeper.net.BookieId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

// TODO assert also on getAdheringToPolicy
@RunWith(Parameterized.class)
public class DefaultEnsemblePlacementPolicyTest {
    private final List<BookieId> expectedResult;
    private final EnsemblePlacementPolicy.PlacementPolicyAdherence expectedPlacement;
    private final int ensembleSize;
    private final int quorumSize;
    private final int ackQuorumSize;
    private final Map<String, byte[]> customMetadata;
    private final Set<BookieId> excludeBookies;

    private static class NewEnsembleParameter {
        private final List<BookieId> expectedResult;
        private final EnsemblePlacementPolicy.PlacementPolicyAdherence expectedPlacement;
        private final int ensembleSize;
        private final int quorumSize;
        private final int ackQuorumSize;
        private final Map<String, byte[]> customMetadata;
        private final Set<BookieId> excludeBookies;

        private NewEnsembleParameter(List<BookieId> expectedResult, EnsemblePlacementPolicy.PlacementPolicyAdherence expectedPlacement,
                                     int ensembleSize, int quorumSize, int ackQuorumSize,
                                     Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies) {
            this.expectedResult = expectedResult;
            this.expectedPlacement = expectedPlacement;

            this.ensembleSize = ensembleSize;
            this.quorumSize = quorumSize;
            this.ackQuorumSize = ackQuorumSize;
            this.customMetadata = customMetadata;
            this.excludeBookies = excludeBookies;
        }
    }

    @Parameterized.Parameters
    public static Collection<NewEnsembleParameter[]> getParameters() {
        return Arrays.asList(new NewEnsembleParameter[][]{
                {new NewEnsembleParameter(new ArrayList<>(), EnsemblePlacementPolicy.PlacementPolicyAdherence.FAIL,
                        0, 0, 0,
                        new HashMap<>(), new HashSet<>())},
        });
    }

    public DefaultEnsemblePlacementPolicyTest(NewEnsembleParameter parameters) {
        this.expectedResult = parameters.expectedResult;
        this.expectedPlacement = parameters.expectedPlacement;

        this.ensembleSize = parameters.ensembleSize;
        this.quorumSize = parameters.quorumSize;
        this.ackQuorumSize = parameters.ackQuorumSize;
        this.customMetadata = parameters.customMetadata;
        this.excludeBookies = parameters.excludeBookies;
    }

    @Test
    public void newEnsembleTest() {
        DefaultEnsemblePlacementPolicy policy = new DefaultEnsemblePlacementPolicy();
        try {
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> result = policy.newEnsemble(this.ensembleSize, this.quorumSize, this.ackQuorumSize, this.customMetadata, this.excludeBookies);
            Assert.assertEquals(this.expectedPlacement, result.getAdheringToPolicy());
        } catch (BKException.BKNotEnoughBookiesException e) {
            throw new RuntimeException(e);
        }
    }
}
