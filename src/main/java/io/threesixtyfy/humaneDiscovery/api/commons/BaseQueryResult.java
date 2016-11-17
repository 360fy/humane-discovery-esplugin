package io.threesixtyfy.humaneDiscovery.api.commons;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class BaseQueryResult implements Streamable, ToXContent {
    private double score = Double.NEGATIVE_INFINITY;

    public BaseQueryResult() {
    }

    public BaseQueryResult(double score) {
        this.score = score;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        score = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(score);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (Double.isNaN(score)) {
            builder.nullField(Fields._SCORE);
        } else {
            builder.field(Fields._SCORE, score);
        }

        buildXContent(builder, params);

        builder.endObject();
        return builder;
    }

    protected abstract void buildXContent(XContentBuilder builder, Params params) throws IOException;

    public static class Fields {
        static final String _SCORE = "_score";
    }
}
