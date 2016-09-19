package io.threesixtyfy.humaneDiscovery.core.encoding;

import io.threesixtyfy.humaneDiscovery.tokenFilter.StringTokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class EdgeGramEncodingUtils {

    private final ESLogger logger = Loggers.getLogger(EdgeGramEncodingUtils.class);

    private final StringTokenStream edgeGramInputTokenStream;
    private final EdgeNGramTokenFilter edgeGramTokenFilter;
    private final CharTermAttribute edgeGramTermAttribute;

    public EdgeGramEncodingUtils() {
        this(2);
    }

    public EdgeGramEncodingUtils(int minSize) {
        edgeGramInputTokenStream = new StringTokenStream();
        edgeGramTokenFilter = new EdgeNGramTokenFilter(edgeGramInputTokenStream, minSize, 20);
        edgeGramTermAttribute = edgeGramTokenFilter.getAttribute(CharTermAttribute.class);
    }

    private Set<String> buildEdgeGramEncodings(String word, Set<String> encodings, String prefix) {
        try {
            edgeGramInputTokenStream.setValue(word);

            edgeGramInputTokenStream.reset();
            edgeGramTokenFilter.reset();

            while (edgeGramTokenFilter.incrementToken()) {
                String edgeGram = edgeGramTermAttribute.toString();
                if (!word.equals(edgeGram)) {
                    if (prefix != null) {
                        edgeGram = prefix + edgeGram;
                    }

                    encodings.add(edgeGram);

//                    buildPhoneticEncodings(edgeGram, encodings, PhoneticEdgeGramPrefix);
                }
            }

            edgeGramTokenFilter.close();
            edgeGramInputTokenStream.close();
        } catch (IOException e) {
            logger.error("IOException in generating edgeGram tokens: {}", e, word);
        }

        return encodings;
    }

    public Set<String> buildEncodings(String word, Set<String> encodings) {
        return buildEdgeGramEncodings(word, encodings, null);
    }

    public Set<String> buildEncodings(String word) {
        return this.buildEncodings(word, new HashSet<>());
    }
}
