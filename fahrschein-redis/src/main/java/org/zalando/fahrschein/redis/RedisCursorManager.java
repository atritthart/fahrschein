package org.zalando.fahrschein.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.zalando.fahrschein.CursorManager;
import org.zalando.fahrschein.StreamKey;
import org.zalando.fahrschein.domain.Cursor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class RedisCursorManager implements CursorManager {
    private static final Logger LOG = LoggerFactory.getLogger(RedisCursorManager.class);

    private final CursorRedisTemplate redisTemplate;
    private final String consumerName;

    public RedisCursorManager(final JedisConnectionFactory jedisConnectionFactory, final String consumerName) {
        this.redisTemplate = new CursorRedisTemplate(jedisConnectionFactory);
        this.consumerName = consumerName;
    }

    @Override
    public void onSuccess(final StreamKey streamKey, final Cursor cursor) throws IOException {
        redisTemplate.opsForValue().set(cursorKey(streamKey, cursor.getPartition()), cursor);
    }

    @Override
    public void onSuccess(final StreamKey streamKey, final List<Cursor> cursors) throws IOException {
        for (Cursor cursor : cursors) {
            onSuccess(streamKey, cursor);
        }
    }

    @Override
    public Collection<Cursor> getCursors(final StreamKey streamKey) throws IOException {
        final RedisCursorKey pattern = new RedisCursorKey(consumerName, streamKey, "*");

        final Set<RedisCursorKey> keys = redisTemplate.keys(pattern);
        final List<Cursor> cursors = new ArrayList<>(keys.size());
        for (RedisCursorKey key : keys) {
            final Cursor cursor = redisTemplate.opsForValue().get(key);
            cursors.add(cursor);
        }

        return cursors;
    }

    private RedisCursorKey cursorKey(final StreamKey streamKey, final String partition) {
        return new RedisCursorKey(consumerName, streamKey, partition);
    }

}
