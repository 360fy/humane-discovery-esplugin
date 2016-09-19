package io.threesixtyfy.humaneDiscovery.api.didYouMean;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseQueryResult;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public class DidYouMeanResult extends BaseQueryResult {

    private String result;

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
        super.readFrom(in);
        result = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(result);
    }

    public static class Fields {
        static final XContentBuilderString RESULT = new XContentBuilderString("result");
    }

    @Override
    protected void buildXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.RESULT, result);
    }
}
