package io.threesixtyfy.humaneDiscovery.es.tokenFilter.phonetic;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.LinkedList;

class DMTokenGenerator extends TokenGenerator {
//        private final ESLogger logger = Loggers.getLogger(DMTokenGenerator.class);

    protected final DoubleMetaphone encoder = new DoubleMetaphone();

    private final LinkedList<State> remainingTokens = new LinkedList<>();

    private boolean generated = false;

    DMTokenGenerator(CharTermAttribute termAtt, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
        super(HumaneTokenFilter.EncodingType.DM.prefix, termAtt, posAtt, payloadAtt);

        encoder.setMaxCodeLen(6);
    }

    @Override
    public boolean incrementToken() throws IOException {
        for (; ; ) {
            if (!remainingTokens.isEmpty()) {
                restoreState(remainingTokens.removeFirst());
                return true;
            }

            // if already generated return false
            if (generated) {
                this.resetState();
                return false;
            }

            int len = this.source == null ? 0 : this.source.length();
            if (len == 0) {
                this.resetState();
                return false; // pass through zero length terms
            }

            int firstAlternativeIncrement = posAtt.getPositionIncrement();

            String primaryPhoneticValue = encoder.doubleMetaphone(source);
            String alternatePhoneticValue = encoder.doubleMetaphone(source, true);

            // one token will be generated.
            if (primaryPhoneticValue != null && primaryPhoneticValue.length() > 0) {
                generated = true;
                posAtt.setPositionIncrement(firstAlternativeIncrement);
                firstAlternativeIncrement = 0;
                termAtt.setEmpty()./*append(this.currPrefix).*/append(primaryPhoneticValue);
                remainingTokens.addLast(captureState());
            }

            if (alternatePhoneticValue != null && alternatePhoneticValue.length() > 0
                    && !alternatePhoneticValue.equals(primaryPhoneticValue)) {
                generated = true;
                posAtt.setPositionIncrement(firstAlternativeIncrement);
                termAtt.setEmpty()./*append(this.currPrefix).*/append(alternatePhoneticValue);
                remainingTokens.addLast(captureState());
            }

            if (remainingTokens.isEmpty()) {
                this.resetState();
                return false;
            }
        }
    }

    private void resetState() {
        generated = false;
        remainingTokens.clear();
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        this.resetState();
    }
}
