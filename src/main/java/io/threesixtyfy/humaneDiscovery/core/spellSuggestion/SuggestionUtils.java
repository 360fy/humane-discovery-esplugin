package io.threesixtyfy.humaneDiscovery.core.spellSuggestion;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

public class SuggestionUtils {

    private static final Logger logger = Loggers.getLogger(SuggestionUtils.class);

    public static boolean noSuggestions(SuggestionSet suggestionSet) {
        return suggestionSet == null || suggestionSet.getSuggestions() == null || suggestionSet.getSuggestions().length == 0;
    }
}
