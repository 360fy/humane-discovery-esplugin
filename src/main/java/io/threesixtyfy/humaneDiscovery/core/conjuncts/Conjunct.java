package io.threesixtyfy.humaneDiscovery.core.conjuncts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Conjunct {
    private final int tokenStart;
    private final int tokenEnd;
    private final int length;
    private final String key;
    //    private final String word;
    private final String[] tokens;

    public Conjunct(int length, String key, /*String word,*/ String[] tokens, int tokenStart, int tokenEnd) {
        this.length = length;
        this.key = key;
//        this.word = word;
        this.tokens = tokens;
        this.tokenStart = tokenStart;
        this.tokenEnd = tokenEnd;
    }

    public static ConjunctBuilder builder() {
        return new ConjunctBuilder();
    }

    public int getTokenStart() {
        return tokenStart;
    }

    public int getTokenEnd() {
        return tokenEnd;
    }

    public String getKey() {
        return key;
    }

//    public String getWord() {
//        return this.word;
//    }

    public int getLength() {
        return length;
    }

    public String[] getTokens() {
        return tokens;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Conjunct conjunct = (Conjunct) o;

        return key.equals(conjunct.key);

    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return "(" + key + ")";
    }

    public static class ConjunctBuilder {
        //        private final StringBuilder word = new StringBuilder();
        private final StringBuilder key = new StringBuilder();
        private final List<String> tokens = new ArrayList<>();
//        private boolean first = true;
        private int length;
        private int tokenStart = -1;
        private int tokenEnd = -1;

        public ConjunctBuilder add(String token, int position) {
            if (tokenStart == -1) {
                tokenStart = position;
                tokenEnd = position;
            } else {
                tokenEnd = position;
            }

//            if (!first) {
//                word.append(" ");
////                key.append("+");
//            }

            // key.append(tokenEnd); //.append(':').append(token);
//            word.append(token);
            key.append(token);

            tokens.add(token);

//            first = false;
            length++;

            return this;
        }

        public ConjunctBuilder add(String prefix, Conjunct conjunct) {
            tokenStart = conjunct.tokenStart - 1;
            tokenEnd = conjunct.tokenEnd;
            // key.append(tokenStart)/*.append(':').append(prefix)*/.append("+").append(conjunct.getKey());
            key.append(prefix)./*append("+").*/append(conjunct.getKey());
//            word.append(prefix).append(" ").append(conjunct.getWord());
            tokens.add(prefix);
            Arrays.stream(conjunct.getTokens()).forEach(tokens::add);
            length = conjunct.length + 1;

            return this;
        }

        public ConjunctBuilder add(Conjunct conjunct, String suffix) {
            tokenStart = conjunct.tokenStart;
            tokenEnd = conjunct.tokenEnd + 1;
            // key.append(conjunct.getKey()).append("+").append(tokenEnd)/*.append(':').append(suffix)*/;
//            word.append(conjunct.getWord()).append(" ").append(suffix);
            key.append(conjunct.getKey())./*append("+").*/append(suffix);
            Arrays.stream(conjunct.getTokens()).forEach(tokens::add);
            tokens.add(suffix);
            length = conjunct.length + 1;

            return this;
        }

        public Conjunct build(Map<String, Conjunct> conjunctMap) {
            String key = this.key.toString();
            if (!conjunctMap.containsKey(key)) {
                conjunctMap.put(key, new Conjunct(length, key, /*word.toString(),*/ tokens.toArray(new String[tokens.size()]), tokenStart, tokenEnd));
            }

            return conjunctMap.get(key);
        }
    }
}
