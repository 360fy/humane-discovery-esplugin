package io.threesixtyfy.humaneDiscovery.core.cache;

//import com.fasterxml.jackson.databind.ObjectMapper;

//import com.lambdaworks.redis.RedisClient;
//import com.lambdaworks.redis.RedisURI;
//import com.lambdaworks.redis.api.StatefulRedisConnection;
//import com.lambdaworks.redis.api.sync.RedisCommands;
//import org.apache.logging.log4j.Logger;
//import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
//import org.elasticsearch.common.io.stream.BytesStreamOutput;
//import org.elasticsearch.common.io.stream.Streamable;
//import org.elasticsearch.common.logging.Loggers;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;

//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.JedisPoolConfig;

//import org.msgpack.jackson.dataformat.JsonArrayFormat;
//import org.msgpack.jackson.dataformat.MessagePackFactory;

//import org.apache.logging.log4j.Logger;
//import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
//import org.elasticsearch.common.io.stream.BytesStreamOutput;
//import org.elasticsearch.common.logging.Loggers;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;

public class CacheService {

//    private static final Logger logger = Loggers.getLogger(CacheService.class);
//
////    private static final CacheService instance = new CacheService();
////
////    public static CacheService instance() {
////        return instance;
////    }
//
////    private final JedisPool jedisPool;
////    private final ObjectMapper objectMapper;
//
////    private final Jedis jedis;
//
////    private final RedisClient redisClient = RedisClient.create(RedisURI.create("localhost", RedisURI.DEFAULT_REDIS_PORT));
////    private final StatefulRedisConnection<String, String> connection = redisClient.connect();
//
//    public CacheService() {
//        // TODO: how to get the redis host and port here...
////        jedisPool = new JedisPool(buildPoolConfig(), "localhost");
////        objectMapper = new ObjectMapper(new MessagePackFactory());
////        objectMapper.setAnnotationIntrospector(new JsonArrayFormat());
////        jedis = new Jedis();
//
//
////        connection.close();
////        redisClient.shutdown();
//    }
//
////    private JedisPoolConfig buildPoolConfig() {
////        final JedisPoolConfig poolConfig = new JedisPoolConfig();
////        poolConfig.setMaxTotal(16);
////        poolConfig.setMaxIdle(8);
////        poolConfig.setMinIdle(2);
////        poolConfig.setTestOnBorrow(true);
////        poolConfig.setTestOnReturn(true);
////        poolConfig.setTestWhileIdle(true);
////        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
////        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
////        poolConfig.setNumTestsPerEvictionRun(3);
////        poolConfig.setBlockWhenExhausted(true);
////        return poolConfig;
////    }
//
//    public <T extends Streamable> void save(String key, T value) throws IOException {
//        RedisCommands<String, String> syncCommands = connection.sync();
//
////        syncCommands.set("key", "Hello, Redis!");
//
////        try (Jedis jedis = jedisPool.getResource()) {
//
//        BytesStreamOutput bout = new BytesStreamOutput();
//        bout.writeString(value.getClass().getName());
//        value.writeTo(bout);
//        bout.close();
//
//        syncCommands.set(key, bout.bytes().utf8ToString());
////        }
//    }
//
//    @SuppressWarnings("unchecked")
//    public <T extends Streamable> T get(String key) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
//        RedisCommands<String, String> syncCommands = connection.sync();
//
//        String utf8Data = syncCommands.get(key);
//        if (utf8Data == null) {
//            return null;
//        }
//
//        byte[] bytes = utf8Data.getBytes("utf8");
//
////        try (Jedis jedis = jedisPool.getResource()) {
//
//
//        ByteBufferStreamInput byteBufferStreamInput = new ByteBufferStreamInput(ByteBuffer.wrap(bytes));
//        String className = byteBufferStreamInput.readString();
//
//        T t = (T) Class.forName(className).newInstance();
//
//        t.readFrom(byteBufferStreamInput);
//
//        logger.info("Retrieved {} from cache = {}", key, t);
//
//        return t;
////        }
//    }
}
