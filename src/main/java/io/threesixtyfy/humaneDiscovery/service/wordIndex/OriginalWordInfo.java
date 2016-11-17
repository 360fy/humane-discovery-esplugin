package io.threesixtyfy.humaneDiscovery.service.wordIndex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OriginalWordInfo {
    private final Map<String, FieldAggregateInfo> fieldAggregateInfo = new HashMap<>();
    private String word;
    private double totalWeight;
    private int totalCount;
    private String display;

    public OriginalWordInfo(String word, String display, double totalWeight, int totalCount) {
        this.word = word;
        this.display = display;
        this.totalWeight = totalWeight;
        this.totalCount = totalCount;
    }

    public Map<String, FieldAggregateInfo> getFieldAggregateInfo() {
        return fieldAggregateInfo;
    }

    public String getWord() {
        return word;
    }

    public double getTotalWeight() {
        return totalWeight;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public String getDisplay() {
        return display;
    }

    @SuppressWarnings("unchecked")
    static OriginalWordInfo unmap(Map<String, Object> map) {
        OriginalWordInfo originalWordInfo = new OriginalWordInfo((String) map.get(WordIndexConstants.WORD_FIELD), (String) map.get(WordIndexConstants.DISPLAY_FIELD), (double) map.get(WordIndexConstants.TOTAL_WEIGHT_FIELD), (int) map.get(WordIndexConstants.TOTAL_COUNT_FIELD));

        List<Map<String, Object>> fieldStats = (List<Map<String, Object>>) map.get(WordIndexConstants.FIELD_STATS_FIELD);
        for (Map<String, Object> stat : fieldStats) {
            originalWordInfo.fieldAggregateInfo.put((String) stat.get(WordIndexConstants.FIELD_NAME_FIELD), FieldAggregateInfo.unmap(stat));
        }

        return originalWordInfo;
    }

    public Map<String, Object> map() {
        Map<String, Object> map = new HashMap<>();

        map.put(WordIndexConstants.WORD_FIELD, word);
        map.put(WordIndexConstants.DISPLAY_FIELD, display);
        map.put(WordIndexConstants.TOTAL_WEIGHT_FIELD, totalWeight);
        map.put(WordIndexConstants.TOTAL_COUNT_FIELD, totalCount);

        map.put(WordIndexConstants.FIELD_STATS_FIELD, fieldAggregateInfo.values().stream().map(FieldAggregateInfo::map).collect(Collectors.toList()));

        return map;
    }

    @Override
    public String toString() {
        return "{" +
                "word='" + word + '\'' +
                ", display='" + display + '\'' +
                ", totalWeight=" + totalWeight +
                ", totalCount=" + totalCount +
                '}';
    }

    public void incrementCount() {
        this.totalCount++;
    }

    public void addWeight(double suggestionWeight) {
        this.totalWeight += suggestionWeight;
    }
}
