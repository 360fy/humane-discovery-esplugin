package io.threesixtyfy.humaneDiscovery.core.conjuncts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Conjunct {
    private final int tokenStart;
    private final int tokenEnd;
    private final int length;
    private final String key;
    private final List<String> tokens;

    public Conjunct(int length, String key, List<String> tokens, int tokenStart, int tokenEnd) {
        this.length = length;
        this.key = key;
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

    public int getLength() {
        return length;
    }

    public List<String> getTokens() {
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
        private boolean first = true;
        private int length;
        private final StringBuilder conjunctKey = new StringBuilder();
        private final List<String> tokens = new ArrayList<>();

        private int tokenStart = -1;
        private int tokenEnd = -1;

        public ConjunctBuilder add(String token, int position) {
            if (tokenStart == -1) {
                tokenStart = position;
                tokenEnd = position;
            } else {
                tokenEnd = position;
            }

            if (!first) {
                conjunctKey.append("+");
            }

            // conjunctKey.append(tokenEnd); //.append(':').append(token);
            conjunctKey.append(token);

            tokens.add(token);

            first = false;
            length++;

            return this;
        }

        public ConjunctBuilder add(String prefix, Conjunct conjunct) {
            tokenStart = conjunct.tokenStart - 1;
            tokenEnd = conjunct.tokenEnd;
            // conjunctKey.append(tokenStart)/*.append(':').append(prefix)*/.append("+").append(conjunct.getKey());
            conjunctKey.append(prefix).append("+").append(conjunct.getKey());
            tokens.add(prefix);
            tokens.addAll(conjunct.getTokens());
            length = conjunct.length + 1;

            return this;
        }

        public ConjunctBuilder add(Conjunct conjunct, String suffix) {
            tokenStart = conjunct.tokenStart;
            tokenEnd = conjunct.tokenEnd + 1;
            // conjunctKey.append(conjunct.getKey()).append("+").append(tokenEnd)/*.append(':').append(suffix)*/;
            conjunctKey.append(conjunct.getKey()).append("+").append(suffix);
            tokens.addAll(conjunct.getTokens());
            tokens.add(suffix);
            length = conjunct.length + 1;

            return this;
        }

        public Conjunct build(Map<String, Conjunct> conjunctMap) {
            String key = conjunctKey.toString();
            if (!conjunctMap.containsKey(key)) {
                conjunctMap.put(key, new Conjunct(length, key, tokens, tokenStart, tokenEnd));
            }

            return conjunctMap.get(key);
        }
    }
}
