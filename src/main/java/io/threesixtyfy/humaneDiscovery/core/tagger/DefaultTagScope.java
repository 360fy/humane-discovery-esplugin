package io.threesixtyfy.humaneDiscovery.core.tagger;

public class DefaultTagScope implements TagScope {

    private final String entityName;

    public DefaultTagScope(String entityName) {
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
