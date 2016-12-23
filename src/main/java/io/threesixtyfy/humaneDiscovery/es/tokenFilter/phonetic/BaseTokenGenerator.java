package io.threesixtyfy.humaneDiscovery.es.tokenFilter.phonetic;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract class BaseTokenGenerator extends TokenGenerator {
//        private final ESLogger logger = Loggers.getLogger(BaseTokenGenerator.class);

    // output is a string such as ab|ac|...
    protected final Pattern pattern;

    // matcher over any buffered output
    protected final Matcher matcher;

    // encoded representation
    protected String encoded;
    // preserves all attributes for any buffered outputs
    protected State state;

    protected boolean used = false;

    BaseTokenGenerator(String prefix, Pattern encodedPattern, CharTermAttribute termAtt, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
        super(prefix, termAtt, posAtt, payloadAtt);

        this.pattern = encodedPattern;
        this.matcher = pattern == null ? null : pattern.matcher("");
    }

    protected abstract String encode();

    protected void setSource(String source) {
        super.setSource(source);

        if (source == null || source.length() < 3) {
            return;
        }

        try {
            encoded = encode();
        } catch (Throwable t) {
            // ignore
        }

        state = captureState();

        if (matcher != null) {
            matcher.reset(encoded);
        }
    }

    private void incrementTokenInternal() {
        assert state != null && encoded != null;
        restoreState(state);

        posAtt.setPositionIncrement(0);
        // payloadAtt.setPayload(new BytesRef(this.source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (encoded == null) {
            return false;
        }

        if (matcher == null && !used) {
            if (this.source.equals(this.encoded)) {
                used = false;

                return false;
            }

            used = true;
            this.incrementTokenInternal();
            termAtt.setEmpty()./*append(this.currPrefix).*/append(encoded);

            return true;
        }

        if (matcher != null && matcher.find()) {
            String value = encoded.substring(matcher.start(1), matcher.end(1));
//                if (this.source.equals(value)) {
//                    continue;
//                }

            this.incrementTokenInternal();
            termAtt.setEmpty()./*append(this.currPrefix).*/append(value);
            return true;
        }

        this.resetState();

        return false;
    }

    private void resetState() {
        used = false;

        encoded = null;

        if (matcher != null) {
            matcher.reset("");
        }

        state = null;
    }

    public void reset() throws IOException {
        super.reset();
        this.resetState();
    }
}
