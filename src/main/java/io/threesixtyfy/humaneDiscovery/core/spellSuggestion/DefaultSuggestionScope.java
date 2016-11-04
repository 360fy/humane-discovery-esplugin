package io.threesixtyfy.humaneDiscovery.core.spellSuggestion;

public class DefaultSuggestionScope implements SuggestionScope {

    private final String entityName;

    public DefaultSuggestionScope(String entityName) {
        this.entityName = entityName;
    }

    @Override
    public String getEntityName() {
        return this.entityName;
    }

    @Override
    public String getEntityClass() {
        return this.entityName;
    }

    @Override
    public float getWeight() {
        return 10;
    }
}
