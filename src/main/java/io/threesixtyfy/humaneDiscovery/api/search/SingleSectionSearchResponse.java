package io.threesixtyfy.humaneDiscovery.api.search;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryResponse;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class SingleSectionSearchResponse extends QueryResponse<SearchResult> {

    private String name;
    private String title;
    private String resultType;

    private static final SearchResult[] EMPTY = new SearchResult[0];

    public SingleSectionSearchResponse(String searchText) {
        super(searchText);
    }

    public SingleSectionSearchResponse(String searchText, SectionResult sectionResult) {
        this(searchText, sectionResult.getName(), sectionResult.getTitle(), sectionResult.getResultType(), sectionResult.getResults(), sectionResult.getTotalResults());
    }

    public SingleSectionSearchResponse(String searchText, String name, String title, String resultType, SearchResult[] results, long totalResults) {
        super(searchText, results, totalResults);

        this.name = name;
        this.title = title;
        this.resultType = resultType;
    }

    public SingleSectionSearchResponse(String searchText, String name, String title, String resultType, SearchResult[] results, long totalResults, int totalShards, int successfulShards, ShardOperationFailedException[] shardFailures) {
        super(searchText, results, totalResults, totalShards, successfulShards, shardFailures);

        this.name = name;
        this.title = title;
        this.resultType = resultType;
    }

    public String getName() {
        return name;
    }

    public String getTitle() {
        return title;
    }

    public String getResultType() {
        return resultType;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setResultType(String resultType) {
        this.resultType = resultType;
    }

    @Override
    protected SearchResult newResult() {
        return new SearchResult();
    }

    @Override
    protected SearchResult[] newResults(int size) {
        return new SearchResult[size];
    }

    @Override
    protected SearchResult[] emptyResults() {
        return EMPTY;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.name = in.readString();
        this.title = in.readString();
        this.resultType = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.name);
        out.writeString(this.title);
        out.writeString(this.resultType);
    }

    @Override
    protected XContentBuilder additionalFields(XContentBuilder builder, Params params) throws IOException {
        if (!StringUtils.isBlank(name)) {
            builder.field(Fields.NAME, name);
        }

        if (!StringUtils.isBlank(title)) {
            builder.field(Fields.TITLE, title);
        }

        if (!StringUtils.isBlank(resultType)) {
            builder.field(Fields.RESULT_TYPE, resultType);
        }

        return builder;
    }

    private static final class Fields {
        static final String TYPE = "type";
        static final String NAME = "name";
        static final String TITLE = "title";
        static final String RESULT_TYPE = "resultType";
    }
}
