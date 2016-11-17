package io.threesixtyfy.humaneDiscovery.tokenFilter;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;

public class StringTokenStream extends TokenStream {
    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
    private boolean used = true;
    private String value = null;

    /**
     * Creates a new TokenStream that returns a String as single token.
     * <p>Warning: Does not initialize the value, you must call
     * {@link #setValue(String)} afterwards!
     */
    public StringTokenStream() {
    }

    /**
     * Sets the string value.
     */
    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean incrementToken() {
        if (used) {
            return false;
        }
        clearAttributes();
        termAttribute.append(value);
        offsetAttribute.setOffset(0, value.length());
        used = true;
        return true;
    }

    @Override
    public void end() throws IOException {
        super.end();
        final int finalOffset = value.length();
        offsetAttribute.setOffset(finalOffset, finalOffset);
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        used = false;
    }

    @Override
    public void close() throws IOException {
        super.close();
        value = null;
    }
}