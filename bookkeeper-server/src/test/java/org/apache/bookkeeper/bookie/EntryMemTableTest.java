package org.apache.bookkeeper.bookie;

import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.utils.ExpectedResult;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class EntryMemTableTest {
    // Testing addEntry and getEntry
    @RunWith(Parameterized.class)
    public static class AddGetEntryTest {
        private final ExpectedResult<Long> expected;
        private final boolean expectedIsEmpty;
        private final long ledgerId;
        private final long entryId;
        private final ByteBuffer entry;
        private final CacheCallback cacheCallback;

        private static EntryMemTable entryMemTable;

        @Parameterized.Parameters
        public static Collection<TestParameters> getParameters() throws IOException {
            CacheCallback nothingCallback = mock(CacheCallback.class);
            doNothing().when(nothingCallback).onSizeLimitReached(any(CheckpointSource.Checkpoint.class));
            CacheCallback throwCallback = mock(CacheCallback.class);
            doThrow(IOException.class).when(throwCallback).onSizeLimitReached(any(CheckpointSource.Checkpoint.class));
            int arbitrarySize = 1024 * 1024;
            return Arrays.asList(
                    new TestParameters(new ExpectedResult<>(null, Exception.class), false, -1, 1, null, throwCallback),
                    new TestParameters(new ExpectedResult<>(0L, null), true, 0, 0, Unpooled.buffer(0).nioBuffer(), nothingCallback),
                    new TestParameters(new ExpectedResult<>((long) arbitrarySize, null), false, 1, -1, ByteBuffer.allocate(arbitrarySize), nothingCallback),
                    new TestParameters(new ExpectedResult<>(null, Exception.class), false, 1, -1, Unpooled.buffer(0).nioBuffer(), null)
            );
        }

        public AddGetEntryTest(TestParameters parameters) {
            this.expected = parameters.expected;
            this.expectedIsEmpty = parameters.expectedIsEmpty;
            this.ledgerId = parameters.ledgerId;
            this.entryId = parameters.entryId;
            this.entry = parameters.entry;
            this.cacheCallback = parameters.cacheCallback;
        }

        @BeforeClass
        public static void setup() {
            CheckpointSource mockedCheckpointSource = mock(CheckpointSource.class);
            entryMemTable = new EntryMemTable(TestBKConfiguration.newServerConfiguration(), mockedCheckpointSource, NullStatsLogger.INSTANCE);
        }

        @AfterClass
        public static void teardown() throws Exception {
            entryMemTable.close();
        }

        @Test
        public void addGetEntryTest() {
            try {
                Long result = entryMemTable.addEntry(this.ledgerId, this.entryId, this.entry, this.cacheCallback);
                EntryKeyValue entryKeyValue = entryMemTable.getEntry(this.ledgerId, this.entryId);
                // Assert for addEntry
                Assert.assertEquals(this.expectedIsEmpty, entryMemTable.isEmpty());
                Assert.assertEquals(this.expected.getResult(), result);
                // Assert for getEntry (whether added data is saved correctly)
                Assert.assertEquals(this.ledgerId, entryKeyValue.getLedgerId());
                Assert.assertEquals(this.entryId, entryKeyValue.getEntryId());
                Assert.assertArrayEquals(this.entry.array(), entryKeyValue.getValueAsByteBuffer().array());
//                Assert.assertEquals(this.entry, entryKeyValue.getValueAsByteBuffer().nioBuffer());
            } catch (Exception e) {
                Assert.assertNotNull(this.expected.getException());
            }
        }

        // Utility Class to have named and typed parameters for the considered test
        private static class TestParameters {
            private final ExpectedResult<Long> expected;
            private final boolean expectedIsEmpty;
            private final long ledgerId;
            private final long entryId;
            private final ByteBuffer entry;
            private final CacheCallback cacheCallback;

            public TestParameters(ExpectedResult<Long> expected, boolean expectedIsEmpty,
                                  long ledgerId, long entryId, ByteBuffer entry, CacheCallback cacheCallback) {
                this.expected = expected;
                this.expectedIsEmpty = expectedIsEmpty;
                this.ledgerId = ledgerId;
                this.entryId = entryId;
                this.entry = entry;
                this.cacheCallback = cacheCallback;
            }
        }
    }

    // Testing getLastEntry
    @RunWith(Parameterized.class)
    public static class GetLastEntryTest {
        private final ExpectedResult<EntryKeyValue> expected;
        private final long ledgerId;

        private static EntryMemTable entryMemTable;
        private static final EntryKeyValue firstLedgerEntry1 = new EntryKeyValue(-1, 0, Unpooled.buffer(1024).array());
        private static final EntryKeyValue firstLedgerEntry2 = new EntryKeyValue(-1, 2, Unpooled.buffer(0).array());
        private static final EntryKeyValue secondLedgerEntry1 = new EntryKeyValue(0, 0, Unpooled.buffer(0).array());
        private static final EntryKeyValue secondLedgerEntry2 = new EntryKeyValue(0, 5, Unpooled.buffer(1024).array());

        @Parameterized.Parameters
        public static Collection<TestParameters> getParameters() {
            return Arrays.asList(
                    new TestParameters(new ExpectedResult<>(firstLedgerEntry2, null), -1),
                    new TestParameters(new ExpectedResult<>(secondLedgerEntry2, null), 0),
                    new TestParameters(new ExpectedResult<>(null, Exception.class), 1)
            );
        }

        public GetLastEntryTest(TestParameters parameters) {
            this.expected = parameters.expected;
            this.ledgerId = parameters.ledgerId;
        }

        @BeforeClass
        public static void setup() throws IOException {
            CheckpointSource mockedCheckpointSource = mock(CheckpointSource.class);
            entryMemTable = new EntryMemTable(TestBKConfiguration.newServerConfiguration(), mockedCheckpointSource, NullStatsLogger.INSTANCE);
            CacheCallback mockCallback = mock(CacheCallback.class);
            entryMemTable.addEntry(firstLedgerEntry1.getLedgerId(), firstLedgerEntry1.getEntryId(), firstLedgerEntry1.getValueAsByteBuffer().nioBuffer(), mockCallback);
            entryMemTable.addEntry(firstLedgerEntry2.getLedgerId(), firstLedgerEntry2.getEntryId(), firstLedgerEntry2.getValueAsByteBuffer().nioBuffer(), mockCallback);
            entryMemTable.addEntry(secondLedgerEntry1.getLedgerId(), secondLedgerEntry1.getEntryId(), secondLedgerEntry1.getValueAsByteBuffer().nioBuffer(), mockCallback);
            entryMemTable.addEntry(secondLedgerEntry2.getLedgerId(), secondLedgerEntry2.getEntryId(), secondLedgerEntry2.getValueAsByteBuffer().nioBuffer(), mockCallback);
        }

        @Test
        public void getLastEntry() {
            try {
                EntryKeyValue result = entryMemTable.getLastEntry(this.ledgerId);
                Assert.assertEquals(this.expected.getResult(), result);
            } catch (IOException e) {
                Assert.assertEquals(this.expected.getException(), e.getClass());
            }
        }

        @AfterClass
        public static void teardown() throws Exception {
            entryMemTable.close();
        }

        // Utility Class to have named and typed parameters for the considered test
        private static class TestParameters {
            private final ExpectedResult<EntryKeyValue> expected;
            private final long ledgerId;

            public TestParameters(ExpectedResult<EntryKeyValue> expected, long ledgerId) {
                this.expected = expected;
                this.ledgerId = ledgerId;
            }
        }
    }
}
