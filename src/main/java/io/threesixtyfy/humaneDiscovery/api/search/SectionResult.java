package io.threesixtyfy.humaneDiscovery.api.search;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseResult;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

public class SectionResult extends BaseResult {
    private static final String SECTION = "section";

    private String name;
    private String title;
    private String resultType;

    private SearchResult[] results;

    private long totalResults;

    public SectionResult() {
    }

    public SectionResult(String name, String title, String resultType, SearchResult[] results, long totalResults) {
        this.name = name;
        this.title = title;
        this.resultType = resultType;
        this.results = results;
        this.totalResults = totalResults;
    }

    public SectionResult(String name, String title, String resultType, SearchHit[] hits, long totalResults) {
        this.name = name;
        this.title = title;
        this.resultType = resultType;
        this.totalResults = totalResults;

        this.results = new SearchResult[hits.length];

        int i = 0;
        for (SearchHit searchHit : hits) {
            this.results[i++] = new SearchResult(searchHit);
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getResultType() {
        return resultType;
    }

    public void setResultType(String resultType) {
        this.resultType = resultType;
    }

    public SearchResult[] getResults() {
        return results;
    }

    public void setResults(SearchResult[] results) {
        this.results = results;
    }

    public long getTotalResults() {
        return totalResults;
    }

    public void setTotalResults(long totalResults) {
        this.totalResults = totalResults;
    }

    @Override
    public long getCount() {
        return this.results == null ? 0 : this.results.length;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        this.title = in.readString();
        this.resultType = in.readString();
        this.totalResults = in.readLong();

        int count = in.readInt();
        this.results = new SearchResult[count];
        for (int i = 0; i < count; i++) {
            this.results[i] = new SearchResult();
            this.results[i].readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.name);
        out.writeString(this.title);
        out.writeString(this.resultType);
        out.writeLong(this.totalResults);
        out.writeInt(this.results == null ? 0 : this.results.length);

        if (this.results != null) {
            for (SearchResult searchResult : this.results) {
                searchResult.writeTo(out);
            }
        }
    }

    @Override
    protected void buildXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TYPE, SECTION);
        builder.field(Fields.NAME, name);
        builder.field(Fields.TITLE, title);
        builder.field(Fields.RESULT_TYPE, resultType);
        builder.field(Fields.TOTAL_RESULTS, totalResults);
        builder.field(Fields.COUNT, results == null ? 0 : this.results.length);

        builder.field(Fields.RESULTS);
        builder.startArray();

        if (results != null) {
            for (SearchResult result : results) {
                result.toXContent(builder, params);
            }
        }

        builder.endArray();
    }

    private static final class Fields {
        static final String TYPE = "type";
        static final String RESULTS = "results";
        static final String NAME = "name";
        static final String TITLE = "title";
        static final String RESULT_TYPE = "resultType";
        static final String TOTAL_RESULTS = "totalResults";
        static final String COUNT = "count";
    }
}
