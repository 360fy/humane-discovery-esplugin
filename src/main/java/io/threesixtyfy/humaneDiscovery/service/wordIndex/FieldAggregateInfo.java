package io.threesixtyfy.humaneDiscovery.service.wordIndex;

import java.util.HashMap;
import java.util.Map;

public class FieldAggregateInfo {

    private double totalWeight;
    private int totalCount;
    private int countAsFullWord;
    private int countAsEdgeGram;
    private String fieldName;

    public FieldAggregateInfo(String fieldName, double totalWeight, int totalCount, int countAsFullWord, int countAsEdgeGram) {
        this.fieldName = fieldName;
        this.totalWeight = totalWeight;
        this.totalCount = totalCount;
        this.countAsFullWord = countAsFullWord;
        this.countAsEdgeGram = countAsEdgeGram;
    }

    public double getTotalWeight() {
        return totalWeight;
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

    public String getFieldName() {
        return fieldName;
    }

    public static FieldAggregateInfo unmap(Map<String, Object> map) {
        int countAsFullWord = 0;
        if (map.containsKey(WordIndexConstants.COUNT_AS_FULL_WORD_FIELD)) {
            countAsFullWord = (int) map.get(WordIndexConstants.COUNT_AS_FULL_WORD_FIELD);
        }

        int countAsEdgeGram = 0;
        if (map.containsKey(WordIndexConstants.COUNT_AS_EDGE_GRAM_FIELD)) {
            countAsEdgeGram = (int) map.get(WordIndexConstants.COUNT_AS_EDGE_GRAM_FIELD);
        }

        return new FieldAggregateInfo((String) map.get(WordIndexConstants.FIELD_NAME_FIELD),
                (double) map.get(WordIndexConstants.TOTAL_WEIGHT_FIELD),
                (int) map.get(WordIndexConstants.TOTAL_COUNT_FIELD),
                countAsFullWord,
                countAsEdgeGram);
    }

    public Map<String, Object> map() {
        Map<String, Object> map = new HashMap<>();

        map.put(WordIndexConstants.FIELD_NAME_FIELD, fieldName);

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

        return map;
    }

    @Override
    public String toString() {
        return "{" +
                ", fieldName='" + fieldName + '\'' +
                ", totalWeight=" + totalWeight +
                ", totalCount=" + totalCount +
                ", countAsFullWord=" + countAsFullWord +
                ", countAsEdgeGram=" + countAsEdgeGram +
                '}';
    }

    public void addWeight(double weight) {
        this.totalWeight += weight;
    }

    public void incrementTotalCount() {
        this.totalCount++;
    }

    public void incrementCountAsEdgeGram() {
        this.countAsEdgeGram++;
    }

    public void incrementCountAsFullWord() {
        this.countAsFullWord++;
    }
}
