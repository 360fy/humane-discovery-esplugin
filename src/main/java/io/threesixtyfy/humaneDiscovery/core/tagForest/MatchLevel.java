package io.threesixtyfy.humaneDiscovery.core.tagForest;

public enum MatchLevel {
    Exact(0, 0),
    EdgeGram(1, 1),
    Phonetic(2, 1),
    EdgeGramPhonetic(3, 1);

    final int code;
    final int level;

    MatchLevel(int code, int level) {
        this.code = code;
        this.level = level;
    }

    public static MatchLevel byCode(int code) {
        for (MatchLevel matchLevel : MatchLevel.values()) {
            if (matchLevel.getCode() == code) {
                return matchLevel;
            }
        }

        return null;
    }

    public int getCode() {
        return code;
    }

    public int getLevel() {
        return level;
    }
}
