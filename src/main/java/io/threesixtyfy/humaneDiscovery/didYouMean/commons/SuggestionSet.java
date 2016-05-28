package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import org.apache.commons.lang3.StringUtils;

public class SuggestionSet {
    private final boolean number;
    private final boolean stopWord;
    private final Suggestion[] suggestions;

    public SuggestionSet(boolean number, boolean stopWord, Suggestion[] suggestions) {
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

    public Suggestion[] getSuggestions() {
        return suggestions;
    }

    @Override
    public String toString() {
        return "{" +
                "N=" + number +
                ",S=" + stopWord +
                ",[]=" + StringUtils.join(suggestions, "\n") +
                '}';
    }
}
