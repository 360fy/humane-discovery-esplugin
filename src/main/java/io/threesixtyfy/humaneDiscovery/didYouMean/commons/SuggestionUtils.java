package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import org.apache.commons.lang3.math.NumberUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class SuggestionUtils {

    private static final ESLogger logger = Loggers.getLogger(SuggestionUtils.class);

    public static boolean noSuggestions(SuggestionSet suggestionSet) {
        return suggestionSet == null || suggestionSet.getSuggestions() == null || suggestionSet.getSuggestions().length == 0;
    }
}
