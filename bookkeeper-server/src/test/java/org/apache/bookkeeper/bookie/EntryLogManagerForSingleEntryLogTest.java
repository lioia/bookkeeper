package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.ExpectedResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

@RunWith(Enclosed.class)
public class EntryLogManagerForSingleEntryLogTest {
    private static EntryLogManagerForSingleEntryLog entryLogManager;

    @BeforeClass
    public static void setup() throws IOException {
        File rootDir = IOUtils.createTempDir("bkTest", ".dir");
        File curDir = BookieImpl.getCurrentDirectory(rootDir);
        BookieImpl.checkDirectoryStructure(curDir);
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        LedgerDirsManager dirsMgr = new LedgerDirsManager(conf, new File[]{rootDir},
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, dirsMgr);
        entryLogManager = (EntryLogManagerForSingleEntryLog) entryLogger.getEntryLogManager();

        // Creating ledgers with id 0 and 1
        createLedger(0, conf);
        createLedger(1, conf);
    }

    private static void createLedger(long ledgerId, ServerConfiguration conf) throws IOException {
        File tmpFileLog0 = File.createTempFile("entrylog", String.valueOf(ledgerId));
        tmpFileLog0.deleteOnExit();
        FileChannel fc = new RandomAccessFile(tmpFileLog0, "rw").getChannel();
        entryLogManager.setCurrentLogForLedgerAndAddToRotate(
                ledgerId,
                new DefaultEntryLogger.BufferedLogChannel(UnpooledByteBufAllocator.DEFAULT, fc,
                        10, 10, 0, tmpFileLog0, conf.getFlushIntervalInBytes())
        );
    }

    @RunWith(Parameterized.class)
    public static class AddEntryTest {
        private final ExpectedResult<EntryKeyValue> expected;
        private final long ledger;
        private final ByteBuf entry;
        private final boolean rollLog;

        public AddEntryTest(TestParameters parameters) {
            this.expected = parameters.expected;
            this.ledger = parameters.ledger;
            this.entry = parameters.entry;
            this.rollLog = parameters.rollLog;
        }

        @Parameterized.Parameters
        public static Collection<TestParameters> getParameters() {
            byte[] b = new byte[1024];
            new Random().nextBytes(b);
            ByteBuf entry = Unpooled.wrappedBuffer(b);
            return Arrays.asList(
                    new TestParameters(new ExpectedResult<>(null, Exception.class), -1, null, true),
                    new TestParameters(new ExpectedResult<>(null, null), 0, Unpooled.buffer(0), false),
                    new TestParameters(new ExpectedResult<>(null, null), 1, entry, true)
            );
        }

        @Test
        public void addEntry() {
            try {
                entryLogManager.addEntry(ledger, entry, rollLog);
            } catch (IOException e) {
                Assert.assertEquals(this.expected.getException(), e.getClass());
            } catch (Exception e) {
                Assert.assertNotNull(this.expected.getException());
            }
        }

        // Utility Class to have named and typed parameters for the considered test
        private static class TestParameters {
            private final ExpectedResult<EntryKeyValue> expected;
            private final long ledger;
            private final ByteBuf entry;
            private final boolean rollLog;

            public TestParameters(ExpectedResult<EntryKeyValue> expected, long ledger, ByteBuf entry, boolean rollLog) {
                this.expected = expected;
                this.ledger = ledger;
                this.entry = entry;
                this.rollLog = rollLog;
            }
        }
    }
}
