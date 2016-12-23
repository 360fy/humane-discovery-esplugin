package io.threesixtyfy.humaneDiscovery.core.encoding;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.threesixtyfy.humaneDiscovery.core.utils.FastObjectPool;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class EncodingsBuilder {

    private static final Logger logger = Loggers.getLogger(EncodingsBuilder.class);

    private final FastObjectPool<EncodingUtils> phoneticEncodingUtilsPool;

    private final Cache<String, Map<String, Set<String>>> CachedEncodings = CacheBuilder
            .newBuilder()
            .maximumSize(1000)
            .build();

    public EncodingsBuilder() {
        phoneticEncodingUtilsPool = new FastObjectPool<>(new EncodingUtils.Factory(), 20);
    }

    public Map<String, Set<String>> encodings(String token, boolean stopWord) {
        try {
            return CachedEncodings.get(token, () -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Building encoding for token: {}", token);
                }

                FastObjectPool.Holder<EncodingUtils> poolHolder = null;
                try {
                    poolHolder = phoneticEncodingUtilsPool.take();
                    if (poolHolder != null && poolHolder.getValue() != null) {
                        return poolHolder.getValue().buildEncodings(token, stopWord);
                    } else {
                        return null;
                    }
                } finally {
                    if (poolHolder != null) {
                        try {
                            phoneticEncodingUtilsPool.release(poolHolder);
                        } catch (Exception ex) {
                            // ignore it
                        }
                    }
                }
            });
        } catch (ExecutionException e) {
            return null;
        }
    }

}
