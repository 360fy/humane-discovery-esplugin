package io.threesixtyfy.humaneDiscovery.commons;

import io.threesixtyfy.humaneDiscovery.tokenFilter.HumaneTokenFilter;
import io.threesixtyfy.humaneDiscovery.tokenFilter.StringTokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class TokenEncodingUtility {

    private final ESLogger logger = Loggers.getLogger(TokenEncodingUtility.class);

    private final StringTokenStream phoneticInputTokenStream = new StringTokenStream();
    private final HumaneTokenFilter phoneticTokenFilter = new HumaneTokenFilter(phoneticInputTokenStream);

    private final CharTermAttribute phoneticTermAttribute = phoneticTokenFilter.getAttribute(CharTermAttribute.class);

    private final StringTokenStream wordTokenStream = new StringTokenStream();
    private final EdgeNGramTokenFilter edgeGramTokenFilter = new EdgeNGramTokenFilter(wordTokenStream, 2, 20);

    private final CharTermAttribute edgeGramTermAttribute = edgeGramTokenFilter.getAttribute(CharTermAttribute.class);

    public List<String> buildEncodings(String word, List<String> phoneticEncodings) {
        try {
            wordTokenStream.setValue(word);

            wordTokenStream.reset();
            edgeGramTokenFilter.reset();

            while (edgeGramTokenFilter.incrementToken()) {
                String edgeGram = edgeGramTermAttribute.toString();
                if (!word.equals(edgeGram)) {
                    phoneticEncodings.add("e#" + edgeGramTermAttribute.toString());
                }
            }

            edgeGramTokenFilter.close();
            wordTokenStream.close();
        } catch (IOException e) {
            logger.error("IOException in generating edgeGram tokens: {}", e, word);
        }

        try {
            phoneticInputTokenStream.setValue(word);

            phoneticInputTokenStream.reset();
            phoneticTokenFilter.reset();

            while (phoneticTokenFilter.incrementToken()) {
                phoneticEncodings.add(phoneticTermAttribute.toString());
            }

            phoneticTokenFilter.close();
            phoneticInputTokenStream.close();
        } catch (IOException e) {
            logger.error("IOException in generating phonetic tokens: {}", e, word);
        }

        return phoneticEncodings;
    }

    public List<String> buildEncodings(String word) {
        return this.buildEncodings(word, new LinkedList<>());
    }
}
