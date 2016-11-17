package io.threesixtyfy.humaneDiscovery.service.wordIndex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class WordAggregateInfo {

    private final String indexName;

    private final String word;
    private final Set<String> encodings = new HashSet<>();
    private final Map<String, FieldAggregateInfo> fieldAggregateInfo = new HashMap<>();
    private final Map<String, OriginalWordInfo> originalWords = new HashMap<>();
    private int totalCount;
    private int countAsFullWord;
    private int countAsEdgeGram;
    private double totalWeight;

    public WordAggregateInfo(String indexName, String word) {
        this.indexName = indexName;
        this.word = word;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getWord() {
        return word;
    }

    public Set<String> getEncodings() {
        return encodings;
    }

    public Map<String, FieldAggregateInfo> getFieldAggregateInfo() {
        return fieldAggregateInfo;
    }

    public Map<String, OriginalWordInfo> getOriginalWords() {
        return originalWords;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public int getCountAsFullWord() {
        return countAsFullWord;
    }

    public int getCountAsEdgeGram() {
        return countAsEdgeGram;
    }

    public double getTotalWeight() {
        return totalWeight;
    }

    @SuppressWarnings("unchecked")
    public static WordAggregateInfo unmap(WordAggregateInfo wordAggregateInfo, Map<String, Object> map) {
        wordAggregateInfo.totalCount = (int) map.get(WordIndexConstants.TOTAL_COUNT_FIELD);
        wordAggregateInfo.totalWeight = (double) map.get(WordIndexConstants.TOTAL_WEIGHT_FIELD);

        if (map.containsKey(WordIndexConstants.COUNT_AS_FULL_WORD_FIELD)) {
            wordAggregateInfo.countAsFullWord = (int) map.get(WordIndexConstants.COUNT_AS_FULL_WORD_FIELD);
        } else {
            wordAggregateInfo.countAsFullWord = 0;
        }

        if (map.containsKey(WordIndexConstants.COUNT_AS_EDGE_GRAM_FIELD)) {
            wordAggregateInfo.countAsEdgeGram = (int) map.get(WordIndexConstants.COUNT_AS_EDGE_GRAM_FIELD);
        } else {
            wordAggregateInfo.countAsEdgeGram = 0;
        }

        List<Map<String, Object>> fieldStats = (List<Map<String, Object>>) map.get(WordIndexConstants.FIELD_STATS_FIELD);
        for (Map<String, Object> stat : fieldStats) {
            wordAggregateInfo.fieldAggregateInfo.put((String) stat.get(WordIndexConstants.FIELD_NAME_FIELD), FieldAggregateInfo.unmap(stat));
        }

        List<Map<String, Object>> originalWordList = (List<Map<String, Object>>) map.get(WordIndexConstants.ORIGINAL_WORDS_FIELD);
        for (Map<String, Object> originalWordMap : originalWordList) {
            OriginalWordInfo originalWordInfo = OriginalWordInfo.unmap(originalWordMap);
            wordAggregateInfo.originalWords.put(originalWordInfo.getWord(), originalWordInfo);
        }

        wordAggregateInfo.encodings.addAll((List<String>) map.get(WordIndexConstants.ENCODINGS_FIELD));

        return wordAggregateInfo;
    }

    @SuppressWarnings("unchecked")
    static WordAggregateInfo unmap(String indexName, Map<String, Object> map) {
        return unmap(new WordAggregateInfo(indexName, (String) map.get(WordIndexConstants.WORD_FIELD)), map);
    }

    public Map<String, Object> map() {
        Map<String, Object> map = new HashMap<>();

        map.put(WordIndexConstants.WORD_FIELD, word);

        if (totalWeight > 0) {
            map.put(WordIndexConstants.TOTAL_WEIGHT_FIELD, totalWeight);
        }

        if (totalCount > 0) {
            map.put(WordIndexConstants.TOTAL_COUNT_FIELD, totalCount);
        }

        if (countAsFullWord > 0) {
            map.put(WordIndexConstants.COUNT_AS_FULL_WORD_FIELD, countAsFullWord);
        }

        if (countAsEdgeGram > 0) {
            map.put(WordIndexConstants.COUNT_AS_EDGE_GRAM_FIELD, countAsEdgeGram);
        }

        map.put(WordIndexConstants.ENCODINGS_FIELD, encodings);
        map.put(WordIndexConstants.FIELD_STATS_FIELD, fieldAggregateInfo.values().stream().map(FieldAggregateInfo::map).collect(Collectors.toList()));

        List<Map<String, Object>> originalWordList = originalWords.values().stream().map(OriginalWordInfo::map).collect(Collectors.toCollection(LinkedList::new));

        map.put(WordIndexConstants.ORIGINAL_WORDS_FIELD, originalWordList);

        return map;
    }

    @Override
    public String toString() {
        return "{" +
                "indexName='" + indexName + '\'' +
                ", word='" + word + '\'' +
                ", totalCount=" + totalCount +
                ", countAsFullWord=" + countAsFullWord +
                ", countAsEdgeGram=" + countAsEdgeGram +
                ", totalWeight=" + totalWeight +
                ", encodings=" + encodings +
                ", fieldAggregateInfo=" + fieldAggregateInfo +
                ", originalWords=" + originalWords +
                '}';
    }

    public void addWeight(double suggestionWeight) {
        this.totalWeight += suggestionWeight;
    }

    public void incrementCount() {
        this.totalCount++;
    }

    public void incrementCountAsEdgeGram() {
        this.countAsEdgeGram++;
    }

    public void incrementCountAsFullWord() {
        this.countAsFullWord++;
    }
}
