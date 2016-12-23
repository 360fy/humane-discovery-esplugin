package io.threesixtyfy.humaneDiscovery.es.tokenFilter.phonetic;

import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.util.regex.Pattern;

class RefinedSoundexTokenGenerator extends BaseTokenGenerator {
    // output is a string such as ab|ac|...
    private static final Pattern pattern = null;
    /**
     * phonetic encoder
     */
    protected final RefinedSoundex encoder = new RefinedSoundex();

    RefinedSoundexTokenGenerator(CharTermAttribute termAtt, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
        super(HumaneTokenFilter.EncodingType.RefinedSoundex.prefix, pattern, termAtt, posAtt, payloadAtt);
    }

    @Override
    protected String encode() {
        return encoder.soundex(this.source);
    }
}
