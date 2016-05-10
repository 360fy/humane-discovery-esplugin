package io.threesixtyfy.humaneDiscovery.didYouMean.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public class DidYouMeanResult implements Streamable, ToXContent {

    private String result;
//    private float score = Float.NEGATIVE_INFINITY;

    public DidYouMeanResult() {
    }

    public DidYouMeanResult(String result) {
        this.result = result;
    }

    public String getResult() {
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
//        score = in.readFloat();
        result = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
//        out.writeFloat(score);
        out.writeString(result);
    }

    public static class Fields {
//        static final XContentBuilderString _SCORE = new XContentBuilderString("_score");
        static final XContentBuilderString RESULT = new XContentBuilderString("result");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

//        if (Float.isNaN(score)) {
//            builder.nullField(Fields._SCORE);
//        } else {
//            builder.field(Fields._SCORE, score);
//        }

        builder.field(Fields.RESULT, result);

        builder.endObject();
        return builder;
    }
}
