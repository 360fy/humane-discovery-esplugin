package io.threesixtyfy.humaneDiscovery.api.commons;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;

import java.io.IOException;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;

public abstract class BaseQueryResponse<T extends BaseQueryResult> extends ActionResponse implements StatusToXContent {

    private static final int TOTAL_SHARDS_DEFAULT = 1;
    private static final int SUCCESSFUL_SHARDS_DEFAULT = 1;
    private static final ShardOperationFailedException[] SHARD_OPERATION_FAILURES_DEFAULT = ShardSearchFailure.EMPTY_ARRAY;

    private int totalShards = TOTAL_SHARDS_DEFAULT;

    private int successfulShards = SUCCESSFUL_SHARDS_DEFAULT;

    private ShardOperationFailedException[] shardFailures = SHARD_OPERATION_FAILURES_DEFAULT;

    private long tookInMillis;

    private boolean timedOut;

    private Boolean terminatedEarly = null;

    private T[] results;

    private long totalResults;

    private float maxScore;

    public BaseQueryResponse() {
        results = emptyResults();
    }

    public BaseQueryResponse(long tookInMillis) {
        this();
        this.tookInMillis = tookInMillis;
    }

    public BaseQueryResponse(T[] results, long tookInMillis) {
        this(results, TOTAL_SHARDS_DEFAULT, SUCCESSFUL_SHARDS_DEFAULT, tookInMillis, SHARD_OPERATION_FAILURES_DEFAULT);
    }

    public BaseQueryResponse(T[] results, int totalShards, int successfulShards, long tookInMillis, ShardOperationFailedException[] shardFailures) {
        this.results = results;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.tookInMillis = tookInMillis;
        this.shardFailures = shardFailures;
        this.totalResults = results == null ? 0 : results.length;
    }

    protected abstract T newResult();

    protected abstract T[] newResults(int size);

    protected abstract T[] emptyResults();

    @Override
    public RestStatus status() {
        return RestStatus.status(successfulShards, totalShards, shardFailures);
    }

    /**
     * Has the search operation timed out.
     */
    public boolean isTimedOut() {
        return timedOut;
    }

    /**
     * Has the search operation terminated early due to reaching
     * <code>terminateAfter</code>
     */
    public Boolean isTerminatedEarly() {
        return terminatedEarly;
    }

    /**
     * How long the search took.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the search took in milliseconds.
     */
    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * The total number of shards the search was executed on.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful number of shards the search was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int getFailedShards() {
        // we don't return totalShards - successfulShards, we don't count "no shards available" as a failed shard, just don't
        // count it in the successful counter
        return shardFailures.length;
    }

    /**
     * The failures that occurred during the search.
     */
    public ShardOperationFailedException[] getShardFailures() {
        return this.shardFailures;
    }

    public T[] getResults() {
        return results;
    }

    public long getTotalResults() {
        return totalResults;
    }

    public float getMaxScore() {
        return maxScore;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TOOK, tookInMillis);
        builder.field(Fields.TIMED_OUT, isTimedOut());
        if (isTerminatedEarly() != null) {
            builder.field(Fields.TERMINATED_EARLY, isTerminatedEarly());
        }
        RestActions.buildBroadcastShardsHeader(builder, params, getTotalShards(), getSuccessfulShards(), getFailedShards(), getShardFailures());
        builder.field(Fields.TOTAL, totalResults);
        if (Float.isNaN(maxScore)) {
            builder.nullField(Fields.MAX_SCORE);
        } else {
            builder.field(Fields.MAX_SCORE, maxScore);
        }
        builder.field(Fields.RESULTS);
        builder.startArray();
        for (T result : results) {
            result.toXContent(builder, params);
        }
        builder.endArray();
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
        totalResults = in.readVLong();
        maxScore = in.readFloat();
        int resultsSize = in.readVInt();
        if (resultsSize == 0) {
            results = emptyResults();
        } else {
            results = newResults(resultsSize);
            for (int i = 0; i < results.length; i++) {
                results[i] = readResult(in);
            }
        }
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        int shardFailureSize = in.readVInt();
        if (shardFailureSize == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[shardFailureSize];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = readShardSearchFailure(in);
            }
        }

        timedOut = in.readBoolean();

        terminatedEarly = in.readOptionalBoolean();

        tookInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(totalResults);
        out.writeFloat(maxScore);
        out.writeVInt(results.length);
        if (results.length > 0) {
            for (T result : results) {
                result.writeTo(out);
            }
        }
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);

        out.writeVInt(shardFailures.length);
        for (ShardOperationFailedException shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }

        out.writeBoolean(timedOut);

        out.writeOptionalBoolean(terminatedEarly);

        out.writeVLong(tookInMillis);
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
        static final String TOOK = "took";
        static final String TIMED_OUT = "timed_out";
        static final String TERMINATED_EARLY = "terminated_early";
        static final String RESULTS = "results";
        static final String TOTAL = "total";
        static final String MAX_SCORE = "max_score";
    }
}