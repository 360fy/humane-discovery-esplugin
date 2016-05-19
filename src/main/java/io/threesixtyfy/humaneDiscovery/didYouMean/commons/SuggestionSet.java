package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import java.util.Set;

public class SuggestionSet {
    private final boolean number;
    private final boolean stopWord;
    private final Set<Suggestion> suggestions;

    public SuggestionSet(boolean number, boolean stopWord, Set<Suggestion> suggestions) {
        this.number = number;
        this.stopWord = stopWord;
        this.suggestions = suggestions;
    }

    public boolean isNumber() {
        return number;
    }

    public boolean isStopWord() {
        return stopWord;
    }

    public Set<Suggestion> getSuggestions() {
        return suggestions;
    }

    @Override
    public String toString() {
        return "{" +
                "N=" + number +
                ",S=" + stopWord +
                ",[]=" + suggestions +
                '}';
    }
}
