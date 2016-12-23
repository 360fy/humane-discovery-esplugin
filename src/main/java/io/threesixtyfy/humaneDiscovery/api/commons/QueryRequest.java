package io.threesixtyfy.humaneDiscovery.api.commons;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class QueryRequest<Q extends QuerySource<Q>, T extends QueryRequest<Q, T>> extends BaseRequest<T> {

    protected final Q querySource;

    public QueryRequest(Q querySource) {
        this.querySource = querySource;
    }

    public QueryRequest(T queryRequest) {
        super(queryRequest);
        this.querySource = queryRequest.querySource;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();

        if (querySource == null) {
            validationException = ValidateActions.addValidationError("query source is missing", null);
        } else {
            validationException = querySource.validate(validationException);
        }

        return validationException;
    }

    public Q querySource() {
        return querySource;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        querySource.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        querySource.writeTo(out);
    }

}
