package org.zalando.fahrschein;

import org.zalando.fahrschein.domain.Cursor;
import org.zalando.fahrschein.domain.Subscription;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Manages cursor offsets for one consumer. One consumer can handle several distinct events.
 */
public interface CursorManager {

    void onSuccess(StreamKey streamKey, Cursor cursor) throws IOException;
    void onSuccess(StreamKey streamKey, List<Cursor> cursors) throws IOException;

    Collection<Cursor> getCursors(StreamKey streamKey) throws IOException;

    default void addSubscription(Subscription subscription) {

    }

    default void addStreamId(Subscription subscription, String streamId) {

    }

}
