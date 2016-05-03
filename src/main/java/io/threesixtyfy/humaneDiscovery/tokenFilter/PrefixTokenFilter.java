package io.threesixtyfy.humaneDiscovery.tokenFilter;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;

public class PrefixTokenFilter extends TokenFilter {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    private final String prefix;

    /**
     * Construct a token stream filtering the given input.
     */
    protected PrefixTokenFilter(TokenStream input, String prefix) {
        super(input);

        this.prefix = prefix;
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (this.input.incrementToken()) {
            // pass through zero-length terms
            if (this.termAtt.length() == 0) {
                return true;
            }

            String input = this.termAtt.toString();

            termAtt.setEmpty().append(prefix).append(input);

            return true;
        } else {
            return false;
        }
    }
}
