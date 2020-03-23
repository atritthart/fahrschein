package org.zalando.fahrschein.redis;

import org.zalando.fahrschein.StreamKey;

class RedisCursorKey {
    private final String consumerName;
    private final StreamKey streamKey;
    private final String partition;

    RedisCursorKey(final String consumerName, final StreamKey streamKey, final String partition) {
        this.consumerName = consumerName;
        this.streamKey = streamKey;
        this.partition = partition;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public String getEventType() {
        return streamKey.getEventName();
    }

    public StreamKey getStreamKey() {
        return streamKey;
    }

    public String getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return "RedisCursorKey{" +
                "consumerName='" + consumerName + '\'' +
                ", streamKey='" + streamKey + '\'' +
                ", partition='" + partition + '\'' +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final RedisCursorKey that = (RedisCursorKey) o;

        if (consumerName != null ? !consumerName.equals(that.consumerName) : that.consumerName != null) return false;
        if (streamKey != null ? !streamKey.equals(that.streamKey) : that.streamKey != null) return false;
        return partition != null ? partition.equals(that.partition) : that.partition == null;

    }

    @Override
    public int hashCode() {
        int result = consumerName != null ? consumerName.hashCode() : 0;
        result = 31 * result + (streamKey != null ? streamKey.hashCode() : 0);
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        return result;
    }
}
