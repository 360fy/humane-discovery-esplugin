package io.threesixtyfy.humaneDiscovery.core.cache;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.SetArgs;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.function.Supplier;

public class CacheService {

//    private static final Logger logger = Loggers.getLogger(CacheService.class);

    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";
    public static final String REDIS_KEY_PREFIX = "redis.key.prefix";
    public static final String DEFAULT_REDIS_HOST = "localhost";
    public static final int DEFAULT_REDIS_PORT = RedisURI.DEFAULT_REDIS_PORT;

    private static final SetArgs SET_ARGS = new SetArgs().ex(300L);

    private final ObjectMapper objectMapper;

    private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;

    private final String redisKeyPrefix;

    @Inject
    public CacheService(Settings settings) {
        this.redisKeyPrefix = settings.get(REDIS_KEY_PREFIX, null);
        RedisClient redisClient = RedisClient.create(RedisURI.create(settings.get(REDIS_HOST, DEFAULT_REDIS_HOST), settings.getAsInt(REDIS_PORT, RedisURI.DEFAULT_REDIS_PORT)));

        pool = AccessController.doPrivileged((PrivilegedAction<GenericObjectPool<StatefulRedisConnection<String, String>>>) () -> ConnectionPoolSupport
                .createGenericObjectPool(redisClient::connect, new GenericObjectPoolConfig(), false));

        objectMapper = new ObjectMapper();
        objectMapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
    }

    private String key(String key) {
        return this.redisKeyPrefix == null ? key : this.redisKeyPrefix + ":" + key;
    }

    public <T> T save(String key, T value) {
        return AccessController.doPrivileged((PrivilegedAction<T>) () -> {
            StatefulRedisConnection<String, String> connection = null;
            try {
                connection = pool.borrowObject();
                connection.sync().set(key(key), objectMapper.writeValueAsString(value), SET_ARGS);

                return value;

            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                pool.returnObject(connection);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> classType) {
        return AccessController.doPrivileged((PrivilegedAction<T>) () -> {
            StatefulRedisConnection<String, String> connection = null;
            try {
                connection = pool.borrowObject();
                String encodedValue = connection.sync().get(key(key));
                if (encodedValue == null) {
                    return null;
                }

                return objectMapper.readValue(encodedValue, classType);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                pool.returnObject(connection);
            }
        });
    }

    public <T> T getOrCompute(String key, Class<T> classType, Supplier<T> supplier) {
        T t = get(key, classType);
        if (t == null) {
            t = supplier.get();
            save(key, t);
        }

        return t;
    }
}
