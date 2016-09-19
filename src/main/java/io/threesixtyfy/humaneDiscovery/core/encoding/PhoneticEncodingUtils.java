package io.threesixtyfy.humaneDiscovery.core.encoding;

import io.threesixtyfy.humaneDiscovery.core.utils.FastObjectPool;
import io.threesixtyfy.humaneDiscovery.tokenFilter.HumaneTokenFilter;
import io.threesixtyfy.humaneDiscovery.tokenFilter.StringTokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static io.threesixtyfy.humaneDiscovery.core.encoding.Constants.GRAM_END_PREFIX;
import static io.threesixtyfy.humaneDiscovery.core.encoding.Constants.GRAM_PREFIX;
import static io.threesixtyfy.humaneDiscovery.core.encoding.Constants.GRAM_START_PREFIX;

public class PhoneticEncodingUtils {

    public static final int MIN_PHONETIC_ENCODING_LENGTH = 3;
    public static final int NO_ENCODING_LENGTH = 1;

    private final ESLogger logger = Loggers.getLogger(PhoneticEncodingUtils.class);

    private final StringTokenStream phoneticInputTokenStream = new StringTokenStream();
    private final HumaneTokenFilter phoneticTokenFilter = new HumaneTokenFilter(phoneticInputTokenStream);
    private final CharTermAttribute phoneticTermAttribute = phoneticTokenFilter.getAttribute(CharTermAttribute.class);

    private Set<String> buildPhoneticEncodings(String word, Set<String> encodings, String prefix, boolean stopWord) {
        if (word == null) {
            return encodings;
        }

        encodings.add(word);

        int wordLength = word.length();

        // we do not add phonetic encodings for 2 or less size
        if (wordLength == NO_ENCODING_LENGTH || stopWord) {
            return encodings;
        }

        int minGram = getMin(wordLength);
        int maxGram = getMax(wordLength);

        for (int ng = 1; ng <= maxGram; ng++) {
            String end = null;
            for (int i = 0; i < wordLength - ng + 1; i++) {
                String gram = word.substring(i, i + ng);

                if (i == 0) {
                    encodings.add(GRAM_START_PREFIX + gram);
                }

                if (ng >= minGram) {
                    encodings.add(GRAM_PREFIX + gram);
                }

                end = gram;
            }

            if (end != null) { // may not be present if len==ng1
                encodings.add(GRAM_END_PREFIX + end);
            }
        }

        if (wordLength < MIN_PHONETIC_ENCODING_LENGTH) {
            return encodings;
        }

        try {
            phoneticInputTokenStream.setValue(word);

            phoneticInputTokenStream.reset();
            phoneticTokenFilter.reset();

            while (phoneticTokenFilter.incrementToken()) {
                String phonetic = phoneticTermAttribute.toString();

                if (prefix != null) {
                    phonetic = prefix + phonetic;
                }

                encodings.add(phonetic);
            }

            phoneticTokenFilter.close();
            phoneticInputTokenStream.close();
        } catch (IOException e) {
            logger.error("IOException in generating phonetic tokens: {}", e, word);
        }

        return encodings;
    }

    public Set<String> buildEncodings(String word, Set<String> encodings, boolean stopWord) {
        return buildPhoneticEncodings(word, encodings, null, stopWord);
    }

    public Set<String> buildEncodings(String word, boolean stopWord) {
        return this.buildEncodings(word, new HashSet<>(), stopWord);
    }

    private int getMin(int l) {
//        if (l > 5) {
//            return 3;
//        }
        if (l >= 5) {
            return 2;
        }

        return 1;
    }

    private int getMax(int l) {
        if (l > 5) {
            return 4;
        }

        if (l == 5) {
            return 3;
        }

        return 2;
    }

//    public static class Factory extends BasePooledObjectFactory<PhoneticEncodingUtils> {
//
//        @Override
//        public PhoneticEncodingUtils create() throws Exception {
//            return new PhoneticEncodingUtils();
//        }
//
//        @Override
//        public PooledObject<PhoneticEncodingUtils> wrap(PhoneticEncodingUtils phoneticEncodingUtils) {
//            return new DefaultPooledObject<>(phoneticEncodingUtils);
//        }
//    }

    public static class Factory implements FastObjectPool.PoolFactory<PhoneticEncodingUtils> {

        @Override
        public PhoneticEncodingUtils create() {
            return new PhoneticEncodingUtils();
        }
    }
}


