package io.threesixtyfy.humaneDiscovery.tokenFilter;

import org.apache.commons.codec.language.DaitchMokotoffSoundex;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.bm.Languages;
import org.apache.commons.codec.language.bm.NameType;
import org.apache.commons.codec.language.bm.PhoneticEngine;
import org.apache.commons.codec.language.bm.RuleType;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HumaneTokenFilter extends TokenFilter {
//    private final ESLogger logger = Loggers.getLogger(HumaneTokenFilter.class);

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    private String inputToken = null;
    private boolean generateHumaneTokens = false;

    private static final int MAX_TOKEN_GENERATOR_INDEX = 3;
    private int currentTokenGeneratorIndex = 0;

    private final TokenGenerator[] tokenGenerators;

    /**
     * Construct a token stream filtering the given input.
     */
    protected HumaneTokenFilter(TokenStream input) {
        super(input);

        final PositionIncrementAttribute posAtt = addAttribute(PositionIncrementAttribute.class);
        final PayloadAttribute payloadAtt = null; //addAttribute(PayloadAttribute.class);

        tokenGenerators = new TokenGenerator[]{
                new RefinedSoundexTokenGenerator(termAtt, posAtt, payloadAtt),
                new DMSoundexTokenGenerator(termAtt, posAtt, payloadAtt),
                new BMTokenGenerator(termAtt, posAtt, payloadAtt),
                new DMTokenGenerator(termAtt, posAtt, payloadAtt)

        };
    }

    // implement filter to encode with BM, DMSoundex, RefinedSoundex encoders
    // add positional information
    // add payload for original word
    @Override
    public boolean incrementToken() throws IOException {
        if (this.generateHumaneTokens && currentTokenGeneratorIndex <= MAX_TOKEN_GENERATOR_INDEX) {
            while (currentTokenGeneratorIndex <= MAX_TOKEN_GENERATOR_INDEX) {
                TokenGenerator currentTokenGenerator = tokenGenerators[currentTokenGeneratorIndex];
                boolean ret = currentTokenGenerator.incrementToken();
                if (!ret) {
                    if (currentTokenGeneratorIndex <= MAX_TOKEN_GENERATOR_INDEX) {
                        currentTokenGeneratorIndex++;

                        if (currentTokenGeneratorIndex < this.tokenGenerators.length) {
                            this.tokenGenerators[currentTokenGeneratorIndex].setSource(this.inputToken);
                        }
                    } else {
                        this.resetState();
                        return false;
                    }
                } else {
                    return true;
                }
            }
        }

        if (this.input.incrementToken()) {
            // pass through less-than-3-length terms
            if (this.termAtt.length() < 3) {
                this.resetState();
                return true;
            }

            this.resetState();

            this.inputToken = this.termAtt.toString();
            this.generateHumaneTokens = true;
            this.tokenGenerators[currentTokenGeneratorIndex].setSource(this.inputToken);

            return true;
        } else {
            return false;
        }
    }

    private void resetState() {
        this.inputToken = null;
        this.generateHumaneTokens = false;
        this.currentTokenGeneratorIndex = 0;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        this.resetState();
    }

    enum TokenTypes {
        RefinedSoundex("rs"),
        DMSoundex("ds"),
        BM("bm"),
        DM("dm");

//        EdgeGram("em"),
//        EdgeGram_DMSoundex("eds"),
//        EdgeGram_RefinedSoundex("ers"),
//        EdgeGram_BM("ebm"),
//        EdgeGram_DM("edm");

        String prefix;

        TokenTypes(String prefix) {
            this.prefix = prefix;
        }
    }

    static abstract class TokenGenerator extends TokenStream {
        protected final CharTermAttribute termAtt;
        protected final PositionIncrementAttribute posAtt;
        protected final PayloadAttribute payloadAtt;

        protected String prefix;

        protected String currPrefix;

        protected String source;

        TokenGenerator(String prefix, CharTermAttribute termAtt, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
            super();
            this.termAtt = termAtt;
            this.posAtt = posAtt;
            this.payloadAtt = payloadAtt;

            this.prefix = this.currPrefix = prefix + "#";
        }

        protected void setSource(String source) {
            if (source.startsWith("e#")) {
                source = source.substring(2);
                this.currPrefix = this.prefix + "e#";
            } else {
                this.currPrefix = this.prefix;
            }

            this.source = source;
        }
    }

    static abstract class BaseTokenGenerator extends TokenGenerator {
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
                termAtt.setEmpty().append(this.currPrefix).append(encoded);

                return true;
            }

            if (matcher != null && matcher.find()) {
                String value = encoded.substring(matcher.start(1), matcher.end(1));
//                if (this.source.equals(value)) {
//                    continue;
//                }

                this.incrementTokenInternal();
                termAtt.setEmpty().append(this.currPrefix).append(value);
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

    static class RefinedSoundexTokenGenerator extends BaseTokenGenerator {
        /**
         * phonetic encoder
         */
        protected final RefinedSoundex encoder = new RefinedSoundex();

        // output is a string such as ab|ac|...
        private static final Pattern pattern = null;

        RefinedSoundexTokenGenerator(CharTermAttribute termAtt, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
            super(TokenTypes.RefinedSoundex.prefix, pattern, termAtt, posAtt, payloadAtt);
        }

        @Override
        protected String encode() {
            return encoder.soundex(this.source);
        }
    }

    static class DMSoundexTokenGenerator extends BaseTokenGenerator {
        /**
         * phonetic encoder
         */
        protected final DaitchMokotoffSoundex encoder = new DaitchMokotoffSoundex();

        // output is a string such as ab|ac|...
        private static final Pattern pattern = Pattern.compile("([^|]+)");

        DMSoundexTokenGenerator(CharTermAttribute termAtt, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
            super(TokenTypes.DMSoundex.prefix, pattern, termAtt, posAtt, payloadAtt);
        }

        @Override
        protected String encode() {
            return encoder.soundex(this.source);
        }
    }

    static class BMTokenGenerator extends BaseTokenGenerator {
        private final PhoneticEngine engine = new PhoneticEngine(NameType.GENERIC, RuleType.APPROX, true);
        private final Languages.LanguageSet languages = Languages.ANY_LANGUAGE;

        // output is a string such as ab|ac|...
        // in complex cases like d'angelo it's (anZelo|andZelo|...)-(danZelo|...)
        // if there are multiple 's, it starts to nest...
        private static final Pattern pattern = Pattern.compile("([^()|-]+)");

        BMTokenGenerator(CharTermAttribute termAtt, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
            super(TokenTypes.BM.prefix, pattern, termAtt, posAtt, payloadAtt);
        }

        @Override
        protected String encode() {
            return (languages == null) ? engine.encode(source) : engine.encode(source, languages);
        }
    }

    static class DMTokenGenerator extends TokenGenerator {
//        private final ESLogger logger = Loggers.getLogger(DMTokenGenerator.class);

        protected final DoubleMetaphone encoder = new DoubleMetaphone();

        private final LinkedList<State> remainingTokens = new LinkedList<>();

        private boolean generated = false;

        DMTokenGenerator(CharTermAttribute termAtt, PositionIncrementAttribute posAtt, PayloadAttribute payloadAtt) {
            super(TokenTypes.DM.prefix, termAtt, posAtt, payloadAtt);

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
                    termAtt.setEmpty().append(this.currPrefix).append(primaryPhoneticValue);
                    remainingTokens.addLast(captureState());
                }

                if (alternatePhoneticValue != null && alternatePhoneticValue.length() > 0
                        && !alternatePhoneticValue.equals(primaryPhoneticValue)) {
                    generated = true;
                    posAtt.setPositionIncrement(firstAlternativeIncrement);
                    termAtt.setEmpty().append(this.currPrefix).append(alternatePhoneticValue);
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

}
