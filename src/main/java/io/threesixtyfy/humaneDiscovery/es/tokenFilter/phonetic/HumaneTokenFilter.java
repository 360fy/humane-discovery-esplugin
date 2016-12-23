package io.threesixtyfy.humaneDiscovery.es.tokenFilter.phonetic;

import io.threesixtyfy.humaneDiscovery.es.tokenFilter.StringTokenStream;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class HumaneTokenFilter extends TokenFilter {
    private static final Logger logger = Loggers.getLogger(HumaneTokenFilter.class);

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final TokenGenerator tokenGenerator;
    private final StringTokenStream stringTokenStream;
    private final EncodingType encodingType;

    private String inputToken = null;

    public HumaneTokenFilter(EncodingType encodingType) {
        this(new StringTokenStream(), encodingType);
    }

    /**
     * Construct a token stream filtering the given input.
     */
    public HumaneTokenFilter(StringTokenStream input, EncodingType encodingType) {
        super(input);

        this.stringTokenStream = input;
        this.encodingType = encodingType;

        final PositionIncrementAttribute posAtt = addAttribute(PositionIncrementAttribute.class);
        final PayloadAttribute payloadAtt = null; //addAttribute(PayloadAttribute.class);

        this.tokenGenerator = tokenGenerator(encodingType, posAtt, payloadAtt);
    }

    private TokenGenerator tokenGenerator(EncodingType encodingType, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
        switch (encodingType) {
            case RefinedSoundex:
                return new RefinedSoundexTokenGenerator(termAtt, posAtt, payloadAtt);
            case DMSoundex:
                return new DMSoundexTokenGenerator(termAtt, posAtt, payloadAtt);
            case BM:
                return new BMTokenGenerator(termAtt, posAtt, payloadAtt);
            case DM:
                return new DMTokenGenerator(termAtt, posAtt, payloadAtt);
        }

        return null;
    }

    @Override
    public boolean incrementToken() throws IOException {
        return incrementEncoding();
    }

    private boolean incrementEncoding() throws IOException {
        // till token generator generates keep returning
        return tokenGenerator.incrementToken() || incrementInput();
    }

    private boolean incrementInput() throws IOException {
        if (this.input.incrementToken()) {
            // pass through less-than-3-length terms
            if (this.termAtt.length() < 3) {
                this.resetState();
                return false;
            }

            this.resetState();

            this.inputToken = this.termAtt.toString();
            this.tokenGenerator.setSource(this.inputToken);

            // TO INCLUDE SOURCE ATTRIBUTE RETURN TRUE FROM HERE, INSTEAD
            return incrementEncoding();
        }

        return false;
    }

    private void resetState() {
        this.inputToken = null;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        this.resetState();
    }

    public Set<String> buildEncodings(String word) {
        Set<String> encodings = new HashSet<>();
        try {
            stringTokenStream.setValue(word);

            stringTokenStream.reset();
            this.reset();

            while (this.incrementToken()) {
                String phonetic = termAtt.toString();

                encodings.add(phonetic);
            }

            this.close();
            stringTokenStream.close();
        } catch (IOException e) {
            logger.error("IOException in generating {} tokens for word: {}", encodingType, word);
        }

        return encodings;
    }

    public enum EncodingType {
        RefinedSoundex("rs"),
        DMSoundex("ds"),
        BM("bm"),
        DM("dm");

        String prefix;

        EncodingType(String prefix) {
            this.prefix = prefix;
        }
    }

}
