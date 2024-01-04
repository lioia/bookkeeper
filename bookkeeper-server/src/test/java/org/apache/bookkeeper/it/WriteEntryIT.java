package org.apache.bookkeeper.it;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.EntryLogScannerImpl;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.util.*;

import static org.mockito.Mockito.*;

@Ignore
@RunWith(Parameterized.class)
public class WriteEntryIT {
    private final List<File> tempDirs = new ArrayList<>();
    private BookieImpl bookie;
    private DefaultEntryLogger bookieEntryLogger;
    private DefaultEntryLogger independentEntryLogger;

    private final Class<? extends java.lang.Throwable> exception;
    private final ByteBuf entry;
    private final boolean ackBeforeSync;
    private final BookkeeperInternalCallbacks.WriteCallback cb;
    private final Object ctx;
    private final byte[] masterKey;

    File createTempDir() throws IOException {
        File dir = IOUtils.createTempDir("bkTest", ".dir");
        tempDirs.add(dir);
        return dir;
    }

    public WriteEntryIT(TestParameters parameters) {
        this.exception = parameters.exception;
        this.entry = parameters.entry;
        this.ackBeforeSync = parameters.ackBeforeSync;
        this.cb = parameters.cb;
        this.ctx = parameters.ctx;
        this.masterKey = parameters.masterKey;
    }

    @Parameterized.Parameters
    public static Collection<TestParameters> getParameters() {
        BookkeeperInternalCallbacks.WriteCallback throwCallback = mock(BookkeeperInternalCallbacks.WriteCallback.class);
        doThrow(RuntimeException.class).when(throwCallback).writeComplete(anyInt(), anyLong(), anyLong(), any(), any());
        BookkeeperInternalCallbacks.WriteCallback nothingCallback = mock(BookkeeperInternalCallbacks.WriteCallback.class);
        doNothing().when(nothingCallback).writeComplete(anyInt(), anyLong(), anyLong(), any(), any());
        ByteBuf buffer = Unpooled.buffer(16);
        buffer.writeLong(0);
        buffer.writeLong(0);
        return Arrays.asList(
                new TestParameters(Exception.class, null, false, null, null, null),
                new TestParameters(Exception.class, Unpooled.buffer(0), true, throwCallback, null, new byte[0]),
                new TestParameters(Exception.class, buffer, false, nothingCallback, null, "valid password".getBytes())
        );
    }

    @Before
    public void setup() throws Exception {
        File ledgerDir1 = createTempDir();
        File ledgerDir2 = createTempDir();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerStorageClass(SortedLedgerStorage.class.getName());
        conf.setJournalDirName(ledgerDir1.toString());
        conf.setLedgerDirNames(new String[]{ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath()});
        bookie = new TestBookieImpl(conf);
        independentEntryLogger = new DefaultEntryLogger(conf, bookie.getLedgerDirsManager());
        SortedLedgerStorage ledgerStorage = ((SortedLedgerStorage) bookie.getLedgerStorage());
        bookieEntryLogger = ledgerStorage.getEntryLogger();
    }

    @Test
    public void writeEntry() {
        try {
            bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);
            long ledgerId = entry.readLong();
            long entryId = entry.readLong();
            independentEntryLogger.flush();
            bookieEntryLogger.flush();
            List<EntryKey> independentEntries = new ArrayList<>();
            List<EntryKey> bookieEntries = new ArrayList<>();
            EntryLogScannerImpl independentScanner = new EntryLogScannerImpl();
            EntryLogScannerImpl bookieScanner = new EntryLogScannerImpl();
            independentEntryLogger.scanEntryLog(0, independentScanner);
            bookieEntryLogger.scanEntryLog(0, bookieScanner);
            for (long flushedLogId : independentEntryLogger.getFlushedLogIds()) {
                independentEntryLogger.scanEntryLog(flushedLogId, independentScanner);
                independentEntries.addAll(independentScanner.entries());
            }
            for (long flushedLogId : bookieEntryLogger.getFlushedLogIds()) {
                bookieEntryLogger.scanEntryLog(flushedLogId, bookieScanner);
                bookieEntries.addAll(bookieScanner.entries());
            }
            Assert.assertTrue(independentEntries.contains(new EntryKey(ledgerId, entryId)));
            Assert.assertTrue(bookieEntries.contains(new EntryKey(ledgerId, entryId)));
        } catch (Exception e) {
            Assert.assertNotNull(exception);
        }
    }

    @After
    public void teardown() throws IOException {
        for (File tempDir : tempDirs) {
            for (File file : Objects.requireNonNull(tempDir.listFiles())) {
                Files.delete(file.toPath());
            }
            Files.delete(tempDir.toPath());
        }
    }

    public static class TestParameters {
        private final Class<? extends java.lang.Throwable> exception;
        private final ByteBuf entry;
        private final boolean ackBeforeSync;
        private final BookkeeperInternalCallbacks.WriteCallback cb;
        private final Object ctx;
        private final byte[] masterKey;

        public TestParameters(Class<? extends java.lang.Throwable> exception, ByteBuf entry, boolean ackBeforeSync,
                              BookkeeperInternalCallbacks.WriteCallback cb, Object ctx, byte[] masterKey) {
            this.exception = exception;
            this.entry = entry;
            this.ackBeforeSync = ackBeforeSync;
            this.cb = cb;
            this.ctx = ctx;
            this.masterKey = masterKey;
        }
    }
}
