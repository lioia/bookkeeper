package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.EntryLogScannerImpl;
import org.apache.bookkeeper.utils.ExpectedResult;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class DefaultEntryLoggerTest {
    private static DefaultEntryLogger entryLogger;
    private static File rootDir;
    private static final long ENTRY_LOG_SIZE_LIMIT = 1024;

    public static void loggerSetup() throws IOException {
        rootDir = IOUtils.createTempDir("bkTest", ".dir");
        File curDir = BookieImpl.getCurrentDirectory(rootDir);
        BookieImpl.checkDirectoryStructure(curDir);
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setEntryLogSizeLimit(ENTRY_LOG_SIZE_LIMIT);
        LedgerDirsManager dirsMgr = new LedgerDirsManager(conf, new File[]{rootDir},
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        entryLogger = new DefaultEntryLogger(conf, dirsMgr);
    }

    public static void loggerTeardown() throws Exception {
        if (entryLogger != null) entryLogger.close();
        FileUtils.deleteDirectory(rootDir);
    }

    private static ByteBuf createEntry(long ledgerId, long entryId, int entrySize) {
        ByteBuf entry = Unpooled.buffer(entrySize + 8 + 8);
        entry.writeLong(ledgerId);
        entry.writeLong(entryId);
        byte[] bytes = new byte[entrySize];
        new Random().nextBytes(bytes);
        entry.writeBytes(bytes);
        return entry;
    }

    @RunWith(Parameterized.class)
    public static class EntryManagementTests {
        private final ExpectedResult<Void> expected;
        private final long ledgerId;
        private final long entryId;
        private final ByteBuf entry;

        public EntryManagementTests(TestParameters parameters) {
            expected = parameters.expected;
            ledgerId = parameters.ledgerId;
            entryId = parameters.entryId;
            entry = parameters.entry;
        }

        @BeforeClass
        public static void setup() throws IOException {
            loggerSetup();
        }

        @AfterClass
        public static void teardown() throws Exception {
            loggerTeardown();
        }

        // TODO category partition and boundary value analysis
        @Parameterized.Parameters
        public static Collection<TestParameters> getParameters() {
            return Arrays.asList(
                    new TestParameters(new ExpectedResult<>(null, Exception.class), -1, 1, null),
                    new TestParameters(new ExpectedResult<>(null, null), 0, 0, createEntry(0, 0, 0)),
                    new TestParameters(new ExpectedResult<>(null, null), 1, -1, createEntry(1, -1, (int) ENTRY_LOG_SIZE_LIMIT)),
                    new TestParameters(new ExpectedResult<>(null, null), 1, 1, createEntry(1, 1, (int) ENTRY_LOG_SIZE_LIMIT + 1))
            );
        }

        @Test
        public void addReadTest() {
            try {
                long logLocation = entryLogger.addEntry(ledgerId, entry);
                ByteBuf read = entryLogger.readEntry(ledgerId, entryId, logLocation);
                Assert.assertEquals(entry, read);
            } catch (Exception e) {
                Assert.assertNotNull(this.expected.getException());
            }
        }

        // Utility Class to have named and typed parameters for the considered test
        public static class TestParameters {
            private final ExpectedResult<Void> expected;
            private final long ledgerId;
            private final long entryId;
            private final ByteBuf entry;

            private TestParameters(ExpectedResult<Void> expected, long ledgerId, long entryId, ByteBuf entry) {
                this.expected = expected;
                this.ledgerId = ledgerId;
                this.entryId = entryId;
                this.entry = entry;
            }
        }
    }

    public static class LogListTests {
        private static int numberOfRealLogs;

        @Before
        public void setup() throws IOException {
            loggerSetup();
            entryLogger.addEntry(0, createEntry(0, 0, 300));
            entryLogger.addEntry(0, createEntry(0, 1, 300));
            entryLogger.addEntry(1, createEntry(1, 0, 1024));
            entryLogger.addEntry(1, createEntry(1, 1, 1024));
            numberOfRealLogs = (int) Math.ceil((double) (300 + 300 + 1024 + 1024) / ENTRY_LOG_SIZE_LIMIT);
        }

        @After
        public void teardown() throws Exception {
            loggerTeardown();
        }

        @Test
        public void logListTests() {
            try {
                Set<Long> logs = entryLogger.getEntryLogsSet();
                long lastLogId = entryLogger.getPreviousAllocatedEntryLogId();
                Assert.assertEquals(lastLogId + 1, logs.size());
                Set<Long> flushedLogs = entryLogger.getFlushedLogIds();
                Assert.assertArrayEquals(new Long[]{}, flushedLogs.toArray());
                entryLogger.flush();
                flushedLogs = entryLogger.getFlushedLogIds();
                Assert.assertEquals(numberOfRealLogs, flushedLogs.size());
            } catch (IOException e) {
                Assert.fail();
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class EntryScanTest {
        private final ExpectedResult<List<EntryKey>> expected;
        private final long logId;
        private final EntryLogScanner scanner;

        public EntryScanTest(TestParameters parameters) {
            this.expected = parameters.expected;
            this.logId = parameters.logId;
            this.scanner = parameters.scanner;
        }

        @BeforeClass
        public static void setup() throws IOException {
            loggerSetup();
            entryLogger.addEntry(0, createEntry(0, 0, 300));
            entryLogger.addEntry(0, createEntry(0, 1, 300));
            entryLogger.addEntry(1, createEntry(1, 0, 1024));
            entryLogger.addEntry(1, createEntry(1, 1, 1024));
        }

        @AfterClass
        public static void teardown() throws Exception {
            loggerTeardown();
        }

        @Parameterized.Parameters
        public static Collection<TestParameters> getParameters() throws IOException {
            EntryLogScanner throwScanner = mock(EntryLogScanner.class);
            doThrow(IOException.class).when(throwScanner).process(anyLong(), anyLong(), any(ByteBuf.class));
            return Arrays.asList(
                    new TestParameters(new ExpectedResult<>(null, Exception.class), -1, null),
                    new TestParameters(new ExpectedResult<>(Collections.singletonList(new EntryKey(0, 0)), null), 0, new EntryLogScannerImpl()),
                    new TestParameters(new ExpectedResult<>(null, Exception.class), 1, throwScanner)
            );
        }

        @Test
        public void scanEntryTest() {
            try {
                entryLogger.scanEntryLog(logId, scanner);
                EntryLogScannerImpl scannerImpl = (EntryLogScannerImpl) scanner;
                Assert.assertArrayEquals(this.expected.getResult().toArray(), scannerImpl.entries().toArray());
            } catch (Exception e) {
                Assert.assertNotNull(this.expected.getException());
            }
        }

        public static class TestParameters {
            private final ExpectedResult<List<EntryKey>> expected;
            private final long logId;
            private final EntryLogScanner scanner;

            public TestParameters(ExpectedResult<List<EntryKey>> expected, long logId, EntryLogScanner scanner) {
                this.expected = expected;
                this.logId = logId;
                this.scanner = scanner;
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class LogExistsTest {
        private final ExpectedResult<Boolean> expected;
        private final long logId;

        public LogExistsTest(TestParameters parameters) {
            this.expected = parameters.expected;
            this.logId = parameters.logId;
        }

        @BeforeClass
        public static void setup() throws IOException {
            loggerSetup();
            entryLogger.addEntry(0, createEntry(0, 0, 300));
            entryLogger.addEntry(0, createEntry(0, 1, 300));
            entryLogger.addEntry(1, createEntry(1, 0, 1024));
            entryLogger.addEntry(1, createEntry(1, 1, 1024));
        }

        @AfterClass
        public static void teardown() throws Exception {
            loggerTeardown();
        }

        @Parameterized.Parameters
        public static Collection<TestParameters> getParameters() {
            return Arrays.asList(
                    new TestParameters(new ExpectedResult<>(false, null), -1),
                    new TestParameters(new ExpectedResult<>(true, null), 0)
            );
        }

        @Test
        public void logExistsTest() {
            try {
                boolean result = entryLogger.logExists(logId);
                Assert.assertEquals(this.expected.getResult(), result);
                boolean deleted = entryLogger.removeEntryLog(logId);
                Assert.assertEquals(this.expected.getResult(), deleted);
                result = entryLogger.logExists(logId);
                Assert.assertFalse(result);
                Set<Long> logs = entryLogger.getEntryLogsSet();
                Assert.assertFalse(logs.contains(logId));
            } catch (IOException e) {
                Assert.fail();
            }
        }

        public static class TestParameters {
            private final ExpectedResult<Boolean> expected;
            private final long logId;

            public TestParameters(ExpectedResult<Boolean> expected, long logId) {
                this.expected = expected;
                this.logId = logId;
            }
        }
    }
}
