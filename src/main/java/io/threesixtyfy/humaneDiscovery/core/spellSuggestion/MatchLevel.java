package io.threesixtyfy.humaneDiscovery.core.spellSuggestion;

//Uni(0),
//Bi(1),
//ShingleUni(2),
//ShingleBi(3);
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

    public static MatchLevel byLevel(int level) {
        for (MatchLevel matchLevel : MatchLevel.values()) {
            if (matchLevel.getLevel() == level) {
                return matchLevel;
            }
        }

        return null;
    }
}
