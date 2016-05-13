package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Conjunct {
    private final int length;
    private final String key;
    //    private final String compoundWord;
//    private final String shingleWord;
    private final List<String> tokens;

    public Conjunct(int length, String key, /*String compoundWord, String shingleWord,*/ List<String> tokens) {
        this.length = length;
        this.key = key;
//        this.compoundWord = compoundWord;
//        this.shingleWord = shingleWord;
        this.tokens = tokens;
    }

    public static ConjunctBuilder builder() {
        return new ConjunctBuilder();
    }

    public String getKey() {
        return key;
    }

//    public String getCompoundWord() {
//        return compoundWord;
//    }
//
//    public String getShingleWord() {
//        return shingleWord;
//    }

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
        //        private final StringBuilder compoundWord = new StringBuilder();
//        private final StringBuilder shingleWord = new StringBuilder();
        private final List<String> tokens = new ArrayList<>();


        public ConjunctBuilder add(String token) {
            if (!first) {
                conjunctKey.append("+");
//                shingleWord.append("_");
            }

            conjunctKey.append(token);
//            compoundWord.append(token);
//            shingleWord.append(token);
            tokens.add(token);

            first = false;
            length++;

            return this;
        }

        public ConjunctBuilder add(String prefix, Conjunct conjunct) {
            conjunctKey.append(prefix).append("+").append(conjunct.getKey());
//            compoundWord.append(prefix).append(conjunct.getCompoundWord());
//            shingleWord.append(prefix).append("_").append(conjunct.getShingleWord());
            tokens.add(prefix);
            tokens.addAll(conjunct.getTokens());
            length = conjunct.length + 1;
            return this;
        }

        public ConjunctBuilder add(Conjunct conjunct, String suffix) {
            conjunctKey.append(conjunct.getKey()).append("+").append(suffix);
//            compoundWord.append(conjunct.getCompoundWord()).append(suffix);
//            shingleWord.append(conjunct.getShingleWord()).append("_").append(suffix);
            tokens.addAll(conjunct.getTokens());
            tokens.add(suffix);
            length = conjunct.length + 1;
            return this;
        }

        public Conjunct build(Map<String, Conjunct> conjunctMap) {
            String key = conjunctKey.toString();
            if (!conjunctMap.containsKey(key)) {
                conjunctMap.put(key, new Conjunct(length, key, /*compoundWord.toString(), shingleWord.toString(),*/ tokens));
            }

            return conjunctMap.get(key);
        }
    }
}
