package io.threesixtyfy.humaneDiscovery.core.tag;

public enum TagType {
    Intent(0),
    Keyword(0),
    StopWord(0),
    NGram(1);

    int level;

    TagType(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }
}
