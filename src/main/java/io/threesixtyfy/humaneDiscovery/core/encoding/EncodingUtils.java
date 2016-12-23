package io.threesixtyfy.humaneDiscovery.core.encoding;

import io.threesixtyfy.humaneDiscovery.core.utils.FastObjectPool;
import io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants;
import io.threesixtyfy.humaneDiscovery.es.tokenFilter.phonetic.HumaneTokenFilter;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EncodingUtils {

    private static final Logger logger = Loggers.getLogger(EncodingUtils.class);

    private static final int MIN_PHONETIC_ENCODING_LENGTH = 3;
    private static final int NO_ENCODING_LENGTH = 2;

    private final HumaneTokenFilter rsTokenFilter = new HumaneTokenFilter(HumaneTokenFilter.EncodingType.RefinedSoundex);
    private final HumaneTokenFilter dsTokenFilter = new HumaneTokenFilter(HumaneTokenFilter.EncodingType.DMSoundex);
    private final HumaneTokenFilter dmTokenFilter = new HumaneTokenFilter(HumaneTokenFilter.EncodingType.DM);
    private final HumaneTokenFilter bmTokenFilter = new HumaneTokenFilter(HumaneTokenFilter.EncodingType.BM);

    private void buildNGramEncodings(String word, int wordLength, Map<String, Set<String>> encodingsMap) {
        Set<String> encodings = new HashSet<>();
        Set<String> startEncodings = new HashSet<>();
        Set<String> endEncodings = new HashSet<>();

        encodings.add(word);

        int minGram = getMin(wordLength);
        int maxGram = getMax(wordLength);

        for (int ng = 1; ng <= maxGram; ng++) {
            String end = null;
            for (int i = 0; i < wordLength - ng + 1; i++) {
                String gram = word.substring(i, i + ng);

                if (i == 0) {
                    startEncodings.add(gram);
                }

                if (ng >= minGram) {
                    encodings.add(gram);
                }

                end = gram;
            }

            if (end != null) { // may not be present if len==ng1
                endEncodings.add(end);
            }
        }

        encodingsMap.put(TokenIndexConstants.Encoding.NGRAM_ENCODING, encodings);
        encodingsMap.put(TokenIndexConstants.Encoding.NGRAM_START_ENCODING, startEncodings);
        encodingsMap.put(TokenIndexConstants.Encoding.NGRAM_END_ENCODING, endEncodings);
    }

    public void buildEncodings(String word, Map<String, Set<String>> encodings, boolean stopWord) {
        if (word == null) {
            return;
        }

        int wordLength = word.length();

        // we do not add phonetic encodings for 2 or less size
        if (wordLength <= NO_ENCODING_LENGTH || stopWord || NumberUtils.isParsable(word)) {
            return;
        }

        buildNGramEncodings(word, wordLength, encodings);

        if (wordLength < MIN_PHONETIC_ENCODING_LENGTH) {
            return;
        }

        encodings.put(TokenIndexConstants.Encoding.RS_ENCODING, rsTokenFilter.buildEncodings(word));
        encodings.put(TokenIndexConstants.Encoding.DS_ENCODING, dsTokenFilter.buildEncodings(word));
        encodings.put(TokenIndexConstants.Encoding.DM_ENCODING, dmTokenFilter.buildEncodings(word));
        encodings.put(TokenIndexConstants.Encoding.BM_ENCODING, bmTokenFilter.buildEncodings(word));
    }

    public void buildEncodings(String[] words, Map<String, Set<String>> encodings, boolean stopWord) {
        for (String word : words) {
            buildEncodings(word, encodings, stopWord);
        }
    }

    public Map<String, Set<String>> buildEncodings(String word, boolean stopWord) {
        Map<String, Set<String>> encodings = new HashMap<>();
        this.buildEncodings(word, encodings, stopWord);
        return encodings;
    }

    private int getMin(int l) {
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

    public static class Factory implements FastObjectPool.PoolFactory<EncodingUtils> {

        @Override
        public EncodingUtils create() {
            return new EncodingUtils();
        }
    }
}


