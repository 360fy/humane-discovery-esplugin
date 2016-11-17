package io.threesixtyfy.humaneDiscovery.service.wordIndex;

public class WordInfo {
    private final String indexName;
    private final String fieldName;

    private final String word;
    private final boolean edgeGram;
    private final String originalWord;
    private final String originalDisplay;

    private final double suggestionWeight;

    public WordInfo(String indexName, String fieldName, String word, double suggestionWeight, boolean edgeGram, String originalWord) {
        this(indexName, fieldName, word, suggestionWeight, edgeGram, originalWord, null);
    }

    public WordInfo(String indexName, String fieldName, String word, double suggestionWeight, boolean edgeGram, String originalWord, String originalDisplay) {
        this.indexName = indexName;
        this.fieldName = fieldName;
        this.word = word;
        this.suggestionWeight = suggestionWeight;
        this.edgeGram = edgeGram;
        this.originalWord = originalWord;
        this.originalDisplay = originalDisplay;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getWord() {
        return word;
    }

    public boolean isEdgeGram() {
        return edgeGram;
    }

    public String getOriginalWord() {
        return originalWord;
    }

    public String getOriginalDisplay() {
        return originalDisplay;
    }

    public double getSuggestionWeight() {
        return suggestionWeight;
    }

    @Override
    public String toString() {
        return "{" +
                "indexName='" + indexName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", word='" + word + '\'' +
                ", edgeGram=" + edgeGram +
                ", originalWord='" + originalWord + '\'' +
                ", originalDisplay='" + originalDisplay + '\'' +
                ", suggestionWeight=" + suggestionWeight +
                '}';
    }
}
