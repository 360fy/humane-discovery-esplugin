package io.threesixtyfy.humaneDiscovery.core.spellSuggestion;

import io.threesixtyfy.humaneDiscovery.core.conjuncts.Conjunct;
import io.threesixtyfy.humaneDiscovery.core.conjuncts.Disjunct;
import io.threesixtyfy.humaneDiscovery.core.conjuncts.DisjunctsBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IntentBuilder {

    private final ESLogger logger = Loggers.getLogger(IntentBuilder.class);

    private static final IntentBuilder instance = new IntentBuilder();

    private final DisjunctsBuilder disjunctsBuilder = DisjunctsBuilder.INSTANCE();

    private final SpellSuggestionsBuilder spellSuggestionsBuilder = new SpellSuggestionsBuilder();

    private IntentBuilder() {
    }

    public static IntentBuilder INSTANCE() {
        return instance;
    }

    public void buildIntent(Client client, List<String> tokens, String[] indices, String[] intentFields) {
        Map<String, Conjunct> conjunctMap = new HashMap<>();
        Disjunct[] disjuncts = disjunctsBuilder.build(tokens, conjunctMap, 1);

        Map<String, SuggestionSet> suggestionSetMap = spellSuggestionsBuilder.fetchSuggestions(client, conjunctMap.values(), indices, null);

        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>> Spell Suggestions: {}", suggestionSetMap);
    }

//    public Map<String, SuggestionSet> tagTokens(Client client, List<String> tokens, String... indices) {
//        TagTokensTask task = new TagTokensTask(client, indices);
//
//        try {
//            return task.tagTokens(tokens);
//        } catch (InterruptedException | ExecutionException e) {
//            logger.error("Error in fetching tags for tokens: {}, indices: {}", e, tokens, indices);
//        }
//
//        return null;
//    }
}
