package io.threesixtyfy.humaneDiscovery.es.tokenFilter.phonetic;

import org.apache.commons.codec.language.DaitchMokotoffSoundex;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.util.regex.Pattern;

class DMSoundexTokenGenerator extends BaseTokenGenerator {
    // output is a string such as ab|ac|...
    private static final Pattern pattern = Pattern.compile("([^|]+)");
    /**
     * phonetic encoder
     */
    protected final DaitchMokotoffSoundex encoder = new DaitchMokotoffSoundex();

    DMSoundexTokenGenerator(CharTermAttribute termAtt, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
        super(HumaneTokenFilter.EncodingType.DMSoundex.prefix, pattern, termAtt, posAtt, payloadAtt);
    }

    @Override
    protected String encode() {
        return encoder.soundex(this.source);
    }
}
