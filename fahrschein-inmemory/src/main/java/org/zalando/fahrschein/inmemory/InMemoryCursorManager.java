package org.zalando.fahrschein.inmemory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.fahrschein.CursorManager;
import org.zalando.fahrschein.StreamKey;
import org.zalando.fahrschein.domain.Cursor;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryCursorManager implements CursorManager {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryCursorManager.class);

    private final ConcurrentHashMap<StreamKey, ConcurrentHashMap<String, Cursor>> partitionsByEventName = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, Cursor> cursorsByPartition(final StreamKey streamKey) {
        return partitionsByEventName.computeIfAbsent(streamKey, key -> new ConcurrentHashMap<>());
    }

    @Override
    public void onSuccess(final StreamKey streamKey, final Cursor cursor) {
        cursorsByPartition(streamKey).put(cursor.getPartition(), cursor);
    }

    @Override
    public void onSuccess(StreamKey streamKey, List<Cursor> cursors) throws IOException {
        for (Cursor cursor : cursors) {
            onSuccess(streamKey, cursor);
        }
    }

    @Override
    public Collection<Cursor> getCursors(final StreamKey streamKey) {
        return Collections.unmodifiableCollection(cursorsByPartition(streamKey).values());
    }
}
