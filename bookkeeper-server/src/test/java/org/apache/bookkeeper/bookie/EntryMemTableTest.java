package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.utils.ExpectedResult;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
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
        private final long ledgerId;
        private final long entryId;
        private final ByteBuffer entry;
        private final CacheCallback cacheCallback;

        private EntryMemTable entryMemTable;

        @Rule
        public ExpectedException exceptionRule = ExpectedException.none();

        @Parameterized.Parameters
        public static Collection<AddGetEntryParameters> getParameters() throws IOException {
            CacheCallback nothingCallback = mock(CacheCallback.class);
            doNothing().when(nothingCallback).onSizeLimitReached(any(CheckpointSource.Checkpoint.class));
            CacheCallback throwCallback = mock(CacheCallback.class);
            doThrow(IOException.class).when(throwCallback).onSizeLimitReached(any(CheckpointSource.Checkpoint.class));
            int specificSize = 1024 * 1024;
            return Arrays.asList(
                    new AddGetEntryParameters(new ExpectedResult<>(null, Exception.class), -1, -1, null, throwCallback),
                    new AddGetEntryParameters(new ExpectedResult<>(0L, null), 0, 0, ByteBuffer.allocate(0), nothingCallback),
                    new AddGetEntryParameters(new ExpectedResult<>((long) specificSize, null), 1, 1, ByteBuffer.allocate(specificSize), nothingCallback)
            );
        }

        public AddGetEntryTest(AddGetEntryParameters parameters) {
            this.expected = parameters.expected;
            this.ledgerId = parameters.ledgerId;
            this.entryId = parameters.entryId;
            this.entry = parameters.entry;
            this.cacheCallback = parameters.cacheCallback;
        }

        @Before
        public void setup() {
            CheckpointSource mockedCheckpointSource = mock(CheckpointSource.class);
            entryMemTable = new EntryMemTable(TestBKConfiguration.newServerConfiguration(), mockedCheckpointSource, NullStatsLogger.INSTANCE);
        }

        @After
        public void teardown() throws Exception {
            entryMemTable.close();
        }

        @Test
        public void addGetEntryTest() {
            exceptionRule.expect(this.expected.getException());
            try {
                Long result = entryMemTable.addEntry(this.ledgerId, this.entryId, this.entry, this.cacheCallback);
                EntryKeyValue entryKeyValue = entryMemTable.getEntry(this.ledgerId, this.entryId);
                // Assert for addEntry
                Assert.assertFalse(entryMemTable.isEmpty()); // data was correctly added
                Assert.assertEquals(this.expected.getResult(), result);
                // Assert for getEntry (whether added data is saved correctly)
                Assert.assertEquals(this.ledgerId, entryKeyValue.getLedgerId());
                Assert.assertEquals(this.entryId, entryKeyValue.getEntryId());
                Assert.assertEquals(this.entry, entryKeyValue.getValueAsByteBuffer().nioBuffer());
            } catch (IOException e) {
                Assert.assertEquals(this.expected.getException(), e.getClass());
            }
        }

        // Utility Class to have named and typed parameters for the considered test
        private static class AddGetEntryParameters {
            private final ExpectedResult<Long> expected;
            private final long ledgerId;
            private final long entryId;
            private final ByteBuffer entry;
            private final CacheCallback cacheCallback;

            public AddGetEntryParameters(ExpectedResult<Long> expected, long ledgerId, long entryId, ByteBuffer entry, CacheCallback cacheCallback) {
                this.expected = expected;
                this.ledgerId = ledgerId;
                this.entryId = entryId;
                this.entry = entry;
                this.cacheCallback = cacheCallback;
            }
        }
    }
}
