package org.apache.bookkeeper.client;

import org.apache.bookkeeper.net.BookieId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.awt.print.Book;
import java.util.*;

// TODO assert also on getAdheringToPolicy
@RunWith(Parameterized.class)
public class DefaultEnsemblePlacementPolicyTest {
    private final EnsemblePlacementPolicy.PlacementResult<List<BookieId>> expected;
    private final int ensembleSize;
    private final int quorumSize;
    private final int ackQuorumSize;
    private final Map<String, byte[]> customMetadata;
    private final Set<BookieId> excludeBookies;

    private static class NewEnsembleParameter {
        private final EnsemblePlacementPolicy.PlacementResult<List<BookieId>> expected;
        private final int ensembleSize;
        private final int quorumSize;
        private final int ackQuorumSize;
        private final Map<String, byte[]> customMetadata;
        private final Set<BookieId> excludeBookies;

        private NewEnsembleParameter(EnsemblePlacementPolicy.PlacementResult<List<BookieId>> expected,
                                     int ensembleSize, int quorumSize, int ackQuorumSize,
                                     Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies) {
            this.expected = expected;
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
                {new NewEnsembleParameter(
                        EnsemblePlacementPolicy.PlacementResult.of(
                                new ArrayList<>(),
                                EnsemblePlacementPolicy.PlacementPolicyAdherence.FAIL
                        ),
                        0, 0, 0,
                        new HashMap<>(), new HashSet<>())},
        });
    }

    public DefaultEnsemblePlacementPolicyTest(NewEnsembleParameter parameters) {
        this.expected = parameters.expected;

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
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> result = policy.newEnsemble(
                    this.ensembleSize, this.quorumSize, this.ackQuorumSize,
                    this.customMetadata, this.excludeBookies
            );
            Assert.assertEquals(this.expected.getAdheringToPolicy(), result.getAdheringToPolicy());
        } catch (BKException.BKNotEnoughBookiesException e) {
            throw new RuntimeException(e);
        }
    }
}
