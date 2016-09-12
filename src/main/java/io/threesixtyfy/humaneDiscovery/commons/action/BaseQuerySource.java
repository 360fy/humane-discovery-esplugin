package io.threesixtyfy.humaneDiscovery.commons.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.FromXContentBuilder;

import java.io.IOException;

public abstract class BaseQuerySource<T extends BaseQuerySource> implements FromXContentBuilder<T> {

    private String query;

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (query == null) {
            validationException = ValidateActions.addValidationError("query is missing", validationException);
        }

        return validationException;
    }

    public String query() {
        return this.query;
    }

    public BaseQuerySource<T> query(String query) {
        this.query = query;
        return this;
    }

    public void readFrom(StreamInput in) throws IOException {
        query = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(query);
    }

    @Override
    public String toString() {
        return "{" +
                "query='" + query + '\'' +
                '}';
    }
}
