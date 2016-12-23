package io.threesixtyfy.humaneDiscovery.api.commons;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class BaseRequest<T extends BaseRequest<T>> extends ActionRequest<T> {

    protected String instance;

    public BaseRequest() {
    }

    public BaseRequest(T request) {
        super();
        this.instance = request.instance;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (instance == null) {
            validationException = ValidateActions.addValidationError("instance is missing", null);
        }

        return validationException;
    }

    public String instance() {
        return this.instance;
    }

    public BaseRequest<T> instance(String instance) {
        this.instance = instance;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.instance = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.instance);
    }

}
