package io.threesixtyfy.humaneDiscovery.core.spellSuggestion;

public interface SuggestionScope {

    String getEntityName();

    String getEntityClass();

    float getWeight();

}
