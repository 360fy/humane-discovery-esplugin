package io.threesixtyfy.humaneDiscovery.service.wordIndex;

public class BigramWordInfo extends WordInfo {
    private final String word1;
    private final String word2;

    public BigramWordInfo(String indexName, String field, String word1, String word2, double suggestionWeight, boolean edgeGram, String originalWord, String originalDisplay) {
        super(indexName, field, word1 + word2, suggestionWeight, edgeGram, originalWord, originalDisplay);

        this.word1 = word1;
        this.word2 = word2;
    }

    public String getWord1() {
        return word1;
    }

    public String getWord2() {
        return word2;
    }

    @Override
    public String toString() {
        return "{" +
                "word1='" + word1 + '\'' +
                ", word2='" + word2 + '\'' +
                "} " + super.toString();
    }
}
