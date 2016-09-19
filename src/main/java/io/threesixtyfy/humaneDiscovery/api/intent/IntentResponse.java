package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseQueryResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public class IntentResponse extends BaseQueryResponse<IntentResult> {

    public static final IntentResult[] EMPTY = new IntentResult[0];
    public static final String[] EMPTY_TOKENS = new String[0];

    private String[] tokens = EMPTY_TOKENS;

    public IntentResponse() {
        super();
    }

    public IntentResponse(long tookInMillis) {
        super(tookInMillis);
    }

    public IntentResponse(String[] tokens, long tookInMillis) {
        super(tookInMillis);
        this.tokens = tokens;
    }

    public IntentResponse(String[] tokens, IntentResult[] results, long tookInMillis) {
        super(results, tookInMillis);
        this.tokens = tokens;
    }

    public String[] getTokens() {
        return tokens;
    }

    @Override
    protected IntentResult newResult() {
        return new IntentResult();
    }

    @Override
    protected IntentResult[] newResults(int size) {
        return new IntentResult[size];
    }

    @Override
    protected IntentResult[] emptyResults() {
        return EMPTY;
    }

    public static class Fields {
        static final XContentBuilderString TOKENS = new XContentBuilderString("tokens");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);

        builder.array(Fields.TOKENS, tokens);

        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tokens = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(tokens);
    }
}
