package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

public enum MatchLevel {
    Exact(0),
    EdgeGram(1),
    Phonetic(2),
    EdgeGramPhonetic(3),
    Synonym(4);

    final int level;

    MatchLevel(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }
}
