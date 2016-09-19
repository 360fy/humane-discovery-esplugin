package io.threesixtyfy.humaneDiscovery.core.spellSuggestion;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class SuggestionUtils {

    private static final ESLogger logger = Loggers.getLogger(SuggestionUtils.class);

    public static boolean noSuggestions(SuggestionSet suggestionSet) {
        return suggestionSet == null || suggestionSet.getSuggestions() == null || suggestionSet.getSuggestions().length == 0;
    }
}
