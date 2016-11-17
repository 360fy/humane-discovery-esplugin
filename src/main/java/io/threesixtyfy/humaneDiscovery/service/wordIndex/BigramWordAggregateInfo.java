package io.threesixtyfy.humaneDiscovery.service.wordIndex;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BigramWordAggregateInfo extends WordAggregateInfo {

    private final Set<String> word1Encodings = new HashSet<>();
    private final Set<String> word2Encodings = new HashSet<>();
    private final String word1;
    private final String word2;

    public BigramWordAggregateInfo(String indexName, String word1, String word2) {
        super(indexName, word1 + word2);
        this.word1 = word1;
        this.word2 = word2;
    }

    public Set<String> getWord1Encodings() {
        return word1Encodings;
    }

    public Set<String> getWord2Encodings() {
        return word2Encodings;
    }

    public String getWord1() {
        return word1;
    }

    public String getWord2() {
        return word2;
    }

    @SuppressWarnings("unchecked")
    public static BigramWordAggregateInfo unmap(String indexName, Map<String, Object> map) {
        BigramWordAggregateInfo wordAggregateInfo = new BigramWordAggregateInfo(indexName, (String) map.get(WordIndexConstants.WORD_1_FIELD), (String) map.get(WordIndexConstants.WORD_2_FIELD));

        WordAggregateInfo.unmap(wordAggregateInfo, map);

        wordAggregateInfo.word1Encodings.addAll((List<String>) map.get(WordIndexConstants.WORD_1_ENCODINGS_FIELD));
        wordAggregateInfo.word2Encodings.addAll((List<String>) map.get(WordIndexConstants.WORD_2_ENCODINGS_FIELD));

        return wordAggregateInfo;
    }

    public Map<String, Object> map() {
        Map<String, Object> map = super.map();
        map.put(WordIndexConstants.WORD_1_FIELD, word1);
        map.put(WordIndexConstants.WORD_2_FIELD, word2);
        map.put(WordIndexConstants.WORD_1_ENCODINGS_FIELD, word1Encodings);
        map.put(WordIndexConstants.WORD_2_ENCODINGS_FIELD, word2Encodings);

        return map;
    }

    @Override
    public String toString() {
        return "{" +
                "word1='" + word1 + '\'' +
                ", word2='" + word2 + '\'' +
                ", word1Encodings=" + word1Encodings +
                ", word2Encodings=" + word2Encodings +
                "} " + super.toString();
    }
}
