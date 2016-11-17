package io.threesixtyfy.humaneDiscovery.core.spellSuggestion;

public enum TokenType {
    Uni(0),
    Bi(1);

    final int level;

    TokenType(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }
}
