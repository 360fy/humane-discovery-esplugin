package io.threesixtyfy.humaneDiscovery.core.spellSuggestion;

import io.threesixtyfy.humaneDiscovery.core.conjuncts.Conjunct;
import org.elasticsearch.client.Client;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class SuggestionsBuilder {

    public static final String HUMANE_QUERY_ANALYZER = "humane_query_analyzer";
    public static final String DUMMY_FIELD = "dummyField";

    private static final SuggestionsBuilder INSTANCE = new SuggestionsBuilder();

    private final SpellSuggestionsBuilder spellSuggestionsBuilder = new SpellSuggestionsBuilder();

    private SuggestionsBuilder() {
    }

    public static SuggestionsBuilder INSTANCE() {
        return INSTANCE;
    }

    public Map<String, SuggestionSet> fetchSuggestions(Client client, Collection<Conjunct> conjuncts, String[] indices, Set<? extends SuggestionScope> suggestionScopes) {
        return spellSuggestionsBuilder.fetchSuggestions(client, conjuncts, indices, suggestionScopes);
    }
}
