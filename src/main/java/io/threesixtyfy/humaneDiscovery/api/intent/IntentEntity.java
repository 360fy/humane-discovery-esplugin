package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.core.spellSuggestion.SuggestionScope;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class IntentEntity<T extends IntentEntity> implements SuggestionScope {

    private String entityName;

    private String entityClass;

    private float weight = 1.0f;

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getEntityClass() {
        if (this.entityClass == null) {
            return this.entityName;
        }

        return entityClass;
    }

    public void setEntityClass(String entityClass) {
        this.entityClass = entityClass;
    }

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    public void readFrom(StreamInput in) throws IOException {
        this.entityName = in.readString();
        this.weight = in.readFloat();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.entityName);
        out.writeFloat(this.weight);
    }

    @Override
    public String toString() {
        return "entityName='" + entityName + '\'';
    }
}
