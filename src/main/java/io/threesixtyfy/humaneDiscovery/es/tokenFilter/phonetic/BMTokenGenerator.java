package io.threesixtyfy.humaneDiscovery.es.tokenFilter.phonetic;

import org.apache.commons.codec.language.bm.Languages;
import org.apache.commons.codec.language.bm.NameType;
import org.apache.commons.codec.language.bm.PhoneticEngine;
import org.apache.commons.codec.language.bm.RuleType;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.util.regex.Pattern;

class BMTokenGenerator extends BaseTokenGenerator {
    // output is a string such as ab|ac|...
    // in complex cases like d'angelo it's (anZelo|andZelo|...)-(danZelo|...)
    // if there are multiple 's, it starts to nest...
    private static final Pattern pattern = Pattern.compile("([^()|-]+)");
    private final PhoneticEngine engine = new PhoneticEngine(NameType.GENERIC, RuleType.APPROX, true);
    private final Languages.LanguageSet languages = Languages.ANY_LANGUAGE;

    BMTokenGenerator(CharTermAttribute termAtt, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
        super(HumaneTokenFilter.EncodingType.BM.prefix, pattern, termAtt, posAtt, payloadAtt);
    }

    @Override
    protected String encode() {
        return (languages == null) ? engine.encode(source) : engine.encode(source, languages);
    }
}
