package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import org.elasticsearch.client.Client;

import java.util.Collection;
import java.util.Map;

public class SuggestionsBuilder {

    public static final String HUMANE_QUERY_ANALYZER = "humane_query_analyzer";
    public static final String DUMMY_FIELD = "dummyField";

    private static final SuggestionsBuilder instance = new SuggestionsBuilder();

    private final SpellSuggestionsBuilder spellSuggestionsBuilder = new SpellSuggestionsBuilder();

    private SuggestionsBuilder() {
    }

    public static SuggestionsBuilder INSTANCE() {
        return instance;
    }

    public Map<String, SuggestionSet> fetchSuggestions(Client client, Collection<Conjunct> conjuncts, String[] indices, String[] queryTypes, String[] queryFields) {
        return spellSuggestionsBuilder.fetchSuggestions(client, conjuncts, indices, queryTypes, queryFields);
    }
}
