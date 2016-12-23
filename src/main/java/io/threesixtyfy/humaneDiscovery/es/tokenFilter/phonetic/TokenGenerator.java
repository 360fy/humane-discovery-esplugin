package io.threesixtyfy.humaneDiscovery.es.tokenFilter.phonetic;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

abstract class TokenGenerator extends TokenStream {
    protected final CharTermAttribute termAtt;
    protected final PositionIncrementAttribute posAtt;
    protected final PayloadAttribute payloadAtt;

//    protected String prefix;
//
//    protected String currPrefix;

    protected String source;

    TokenGenerator(String prefix, CharTermAttribute termAtt, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
        super();
        this.termAtt = termAtt;
        this.posAtt = posAtt;
        this.payloadAtt = payloadAtt;

//        this.prefix = this.currPrefix = prefix + "#";
    }

    protected void setSource(String source) {
//        if (source.startsWith("e#")) {
//            source = source.substring(2);
//            this.currPrefix = this.prefix + "e#";
//        } else {
//            this.currPrefix = this.prefix;
//        }

        this.source = source;
    }
}
