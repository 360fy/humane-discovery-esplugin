package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class IntentResponse extends QueryResponse<IntentResult> {

    public static final IntentResult[] INTENT_RESULTS_EMPTY = new IntentResult[0];
    public static final String[] TOKENS_EMPTY = new String[0];

    private String[] tokens = TOKENS_EMPTY;

    public IntentResponse(String searchText) {
        super(searchText);
    }

    public IntentResponse(String searchText, String[] tokens) {
        this(searchText);
        this.tokens = tokens;
    }

    public IntentResponse(String searchText, String[] tokens, IntentResult[] results) {
        super(searchText, results, 0);
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
        return INTENT_RESULTS_EMPTY;
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

    public static class Fields {
        static final String TOKENS = "tokens";
    }
}
