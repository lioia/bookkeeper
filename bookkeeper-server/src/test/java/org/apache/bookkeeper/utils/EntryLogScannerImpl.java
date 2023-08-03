package org.apache.bookkeeper.utils;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.bookie.EntryKey;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;

import java.util.ArrayList;
import java.util.List;

public class EntryLogScannerImpl implements EntryLogScanner {
    private final List<EntryKey> entries;

    public EntryLogScannerImpl() {
        entries = new ArrayList<>();
    }

    @Override
    public boolean accept(long ledgerId) {
        return true;
    }

    @Override
    public void process(long ledgerId, long offset, ByteBuf entry) {
        entries.add(new EntryKey(entry.readLong(), entry.readLong()));
    }

    public List<EntryKey> entries() {
        return entries;
    }
}
