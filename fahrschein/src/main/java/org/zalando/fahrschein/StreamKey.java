package org.zalando.fahrschein;

import javax.annotation.Nullable;
import java.util.Objects;

public class StreamKey {
    private final String eventName;

    @Nullable
    private final String subscriptionId;

    public static StreamKey of(final String eventName, final String subscriptionId) {
        return new StreamKey(eventName, subscriptionId);
    }

    private StreamKey(final String eventName, final String subscriptionId) {
        this.eventName = eventName;
        this.subscriptionId = subscriptionId;
    }

    public String getEventName() {
        return eventName;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamKey streamKey = (StreamKey) o;
        return Objects.equals(eventName, streamKey.eventName) &&
                Objects.equals(subscriptionId, streamKey.subscriptionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventName, subscriptionId);
    }
}
