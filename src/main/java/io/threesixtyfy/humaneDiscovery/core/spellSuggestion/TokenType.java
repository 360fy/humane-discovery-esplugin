package io.threesixtyfy.humaneDiscovery.core.spellSuggestion;

public enum TokenType {
    Uni(0),
    Bi(1);

//    ShingleUni(2),
//    ShingleBi(3);

    final int level;

    TokenType(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }
}
