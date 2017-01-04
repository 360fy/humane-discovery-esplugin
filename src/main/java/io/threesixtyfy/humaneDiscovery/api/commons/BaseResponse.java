package io.threesixtyfy.humaneDiscovery.api.commons;

import com.fasterxml.jackson.annotation.JsonIgnore;
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

import java.io.IOException;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;

public abstract class BaseResponse extends ActionResponse implements StatusToXContent {

//    private static final Logger logger = Loggers.getLogger(QueryResponse.class);

    private static final int TOTAL_SHARDS_DEFAULT = 1;
    private static final int SUCCESSFUL_SHARDS_DEFAULT = 1;
    private static final ShardOperationFailedException[] SHARD_OPERATION_FAILURES_DEFAULT = ShardSearchFailure.EMPTY_ARRAY;

    private int totalShards = TOTAL_SHARDS_DEFAULT;

    private int successfulShards = SUCCESSFUL_SHARDS_DEFAULT;

    private ShardOperationFailedException[] shardFailures = SHARD_OPERATION_FAILURES_DEFAULT;

    private long tookInMillis;

    private boolean timedOut;

    private Boolean terminatedEarly = null;

    public BaseResponse() {
        this(TOTAL_SHARDS_DEFAULT, SUCCESSFUL_SHARDS_DEFAULT, SHARD_OPERATION_FAILURES_DEFAULT);
    }

    public BaseResponse(int totalShards, int successfulShards, ShardOperationFailedException[] shardFailures) {
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.shardFailures = shardFailures;
    }

    @Override
    public RestStatus status() {
        return RestStatus.status(successfulShards, totalShards, shardFailures);
    }

    /**
     * Has the search operation timed out.
     */
    @JsonIgnore
    public boolean isTimedOut() {
        return timedOut;
    }

    /**
     * Has the search operation terminated early due to reaching
     * <code>terminateAfter</code>
     */
    @JsonIgnore
    public Boolean isTerminatedEarly() {
        return terminatedEarly;
    }

    /**
     * How long the search took.
     */
    @JsonIgnore
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the search took in milliseconds.
     */
    @JsonIgnore
    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * The total number of shards the search was executed on.
     */
    @JsonIgnore
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful number of shards the search was executed on.
     */
    @JsonIgnore
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The failed number of shards the search was executed on.
     */
    @JsonIgnore
    public int getFailedShards() {
        // we don't return totalShards - successfulShards, we don't count "no shards available" as a failed shard, just don't
        // count it in the successful counter
        return shardFailures.length;
    }

    /**
     * The failures that occurred during the search.
     */
    @JsonIgnore
    public ShardOperationFailedException[] getShardFailures() {
        return this.shardFailures;
    }

    public BaseResponse calculateTookInMillis(long startTime) {
        this.tookInMillis = System.currentTimeMillis() - startTime;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.SERVICE_TIME_TAKEN, Math.round(tookInMillis * 1000.0 / 1000) / 1000.0);
        builder.field(Fields.SERVICE_TIME_TAKEN_IN_MS, tookInMillis);
        builder.field(Fields.TIMED_OUT, isTimedOut());
        if (isTerminatedEarly() != null) {
            builder.field(Fields.TERMINATED_EARLY, isTerminatedEarly());
        }
//        RestActions.buildBroadcastShardsHeader(builder, params, getTotalShards(), getSuccessfulShards(), getFailedShards(), getShardFailures());

        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        int shardFailureSize = in.readVInt();
        if (shardFailureSize <= 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[shardFailureSize];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = readShardSearchFailure(in);
            }
        }

        timedOut = in.readOptionalBoolean();
        terminatedEarly = in.readOptionalBoolean();
        tookInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);

        out.writeVInt(shardFailures.length);
        for (ShardOperationFailedException shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }

        out.writeOptionalBoolean(timedOut);
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
        static final String SERVICE_TIME_TAKEN_IN_MS = "serviceTimeTakenInMs";
        static final String SERVICE_TIME_TAKEN = "serviceTimeTaken";
        static final String TIMED_OUT = "timed_out";
        static final String TERMINATED_EARLY = "terminated_early";
    }
}
