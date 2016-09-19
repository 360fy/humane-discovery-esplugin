package io.threesixtyfy.humaneDiscovery.api.commons;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

public abstract class BaseQueryRequest<R extends BaseQuerySource, T extends BaseQueryRequest<R, T>> extends ActionRequest<T> implements IndicesRequest.Replaceable {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosed();

    protected String[] indices;
    protected String[] types = Strings.EMPTY_ARRAY;
    protected IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    protected final R querySource;

    public BaseQueryRequest() {
        this.querySource = newQuerySource();
    }

    public BaseQueryRequest(T queryRequest, ActionRequest originalRequest) {
        super(originalRequest);
        this.querySource = (R) queryRequest.querySource;
        this.indices = queryRequest.indices;
        this.indicesOptions = queryRequest.indicesOptions;
        this.types = queryRequest.types;
    }

    protected abstract R newQuerySource();

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (indices == null) {
            validationException = ValidateActions.addValidationError("indices is missing", null);
        }

        if (querySource == null) {
            validationException = ValidateActions.addValidationError("query source is missing", null);
        } else {
            validationException = querySource.validate(validationException);
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

    public String[] types() {
        return types;
    }

    public BaseQueryRequest<R, T> types(String... types) {
        this.types = types;
        return this;
    }

    public BaseQueryRequest<R, T> indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return this.indicesOptions;
    }

    public R querySource() {
        return querySource;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        indices = new String[in.readVInt()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = in.readString();
        }

        types = in.readStringArray();
        querySource.readFrom(in);
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indices.length);
        for (String index : indices) {
            out.writeString(index);
        }

        out.writeStringArray(types);
        querySource.writeTo(out);
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public String toString() {
        return "{" +
                "indices=" + Arrays.toString(indices) +
                ", types=" + Arrays.toString(types) +
                ", indicesOptions=" + indicesOptions +
                ", querySource=" + querySource +
                "} " + super.toString();
    }
}
