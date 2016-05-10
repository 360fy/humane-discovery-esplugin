package io.threesixtyfy.humaneDiscovery.didYouMean.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DidYouMeanRequest extends ActionRequest<DidYouMeanRequest> implements IndicesRequest.Replaceable {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosed();

    private String[] indices;
    private String[] types = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
    private String query;

    public DidYouMeanRequest() {
    }

    public DidYouMeanRequest(DidYouMeanRequest didYouMeanRequest, ActionRequest originalRequest) {
        super(originalRequest);
        this.indices = didYouMeanRequest.indices;
        this.types = didYouMeanRequest.types;
        this.indicesOptions = didYouMeanRequest.indicesOptions;
        this.query = didYouMeanRequest.query;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null) {
            validationException = ValidateActions.addValidationError("indices is missing", validationException);
        }
        if (query == null) {
            validationException = ValidateActions.addValidationError("query is missing", validationException);
        }

        return validationException;
    }

    @Override
    public IndicesRequest indices(String[] indices) {
        if (indices == null) {
            throw new IllegalArgumentException("indices must not be null");
        } else {
            for (int i = 0; i < indices.length; i++) {
                if (indices[i] == null) {
                    throw new IllegalArgumentException("indices[" + i + "] must not be null");
                }
            }
        }
        this.indices = indices;
        return this;
    }

    @Override
    public String[] indices() {
        return this.indices;
    }

    public DidYouMeanRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return this.indicesOptions;
    }

    public String[] types() {
        return types;
    }

    public DidYouMeanRequest types(String... types) {
        this.types = types;
        return this;
    }

    public String query() {
        return this.query;
    }

    public DidYouMeanRequest query(String query) {
        this.query = query;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        indices = new String[in.readVInt()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = in.readString();
        }

        query = in.readString();

        types = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indices.length);
        for (String index : indices) {
            out.writeString(index);
        }

        out.writeString(query);

        out.writeStringArray(types);
        indicesOptions.writeIndicesOptions(out);
    }
}
