package io.threesixtyfy.humaneDiscovery.commons.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public abstract class BaseQueryResult implements Streamable, ToXContent {
    private float score = Float.NEGATIVE_INFINITY;

    @Override
    public void readFrom(StreamInput in) throws IOException {
        score = in.readFloat();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(score);
    }

    public static class Fields {
        static final XContentBuilderString _SCORE = new XContentBuilderString("_score");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (Float.isNaN(score)) {
            builder.nullField(Fields._SCORE);
        } else {
            builder.field(Fields._SCORE, score);
        }

        buildXContent(builder, params);

        builder.endObject();
        return builder;
    }

    protected abstract void buildXContent(XContentBuilder builder, Params params) throws IOException;
}
