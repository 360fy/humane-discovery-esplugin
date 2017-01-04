package io.threesixtyfy.humaneDiscovery.api.commons;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public abstract class QueryResponse<T extends BaseResult> extends BaseResponse {

//    private static final Logger logger = Loggers.getLogger(QueryResponse.class);

    private String searchText;

    private T[] results;

    private long totalResults;

    private long count;

    public QueryResponse() {
        super();
    }

//    public QueryResponse(String searchText) {
//        this(searchText, emptyResults(), 0);
//        results = emptyResults();
//        this.searchText = searchText;
//    }

    public QueryResponse(String searchText, T[] results, long totalResults) {
        super();
        this.searchText = searchText;
        this.results = results;
        this.totalResults = totalResults;

        calculateCount(this.results);
    }

    public QueryResponse(String searchText, T[] results, long totalResults, int totalShards, int successfulShards, ShardOperationFailedException[] shardFailures) {
        super(totalShards, successfulShards, shardFailures);
        this.searchText = searchText;
        this.results = results;
        this.totalResults = totalResults;

        calculateCount(this.results);
    }

    protected abstract T newResult();

    protected abstract T[] newResults(int size);

    protected abstract T[] emptyResults();

    public String getSearchText() {
        return searchText;
    }

    public long getCount() {
        return count;
    }

    public T[] getResults() {
        return results;
    }

    public long getTotalResults() {
        return totalResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);

        additionalFields(builder, params);
        builder.field(Fields.SEARCH_TEXT, this.searchText);
        builder.field(Fields.COUNT, this.count);
        builder.field(Fields.TOTAL_RESULTS, totalResults);
        builder.field(Fields.RESULTS);
        builder.startArray();

        if (results != null) {
            for (T result : results) {
                result.toXContent(builder, params);
            }
        }

        builder.endArray();

        return builder;
    }

    protected XContentBuilder additionalFields(XContentBuilder builder, Params params) throws IOException {
        // do nothing method
        return builder;
    }

    private T readResult(StreamInput in) throws IOException {
        T result = newResult();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.searchText = in.readString();
        this.count = in.readVLong();
        this.totalResults = in.readVLong();
        int resultsSize = in.readVInt();

        if (resultsSize <= 0) {
            this.results = emptyResults();
        } else {
            this.results = newResults(resultsSize);
            for (int i = 0; i < this.results.length; i++) {
                this.results[i] = readResult(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.searchText);
        out.writeVLong(this.count);
        out.writeVLong(this.totalResults);

        out.writeVInt(this.results.length);
        if (this.results.length > 0) {
            for (T result : results) {
                result.writeTo(out);
            }
        }
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    private static final class Fields {
        static final String SEARCH_TEXT = "searchText";
        static final String RESULTS = "results";
        static final String TOTAL_RESULTS = "totalResults";
        static final String COUNT = "count";
    }

    private void calculateCount(T[] results) {
        if (results == null) {
            this.count = 0;
        } else {
            for (T result : results) {
                this.count += result.getCount();
            }
        }
    }
}
