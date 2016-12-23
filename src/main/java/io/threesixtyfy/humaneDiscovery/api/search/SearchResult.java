package io.threesixtyfy.humaneDiscovery.api.search;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseResult;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Map;

public class SearchResult extends BaseResult {

    private String id;
    private String type;
    private float score;

    private BytesReference source;

    private Map<String, Object> sourceAsMap;

    public SearchResult() {
    }

    public SearchResult(SearchHit searchHit) {
        this(searchHit.id(), searchHit.type(), searchHit.score(), searchHit.sourceRef());
    }

    public SearchResult(String id, String type, float score, BytesReference source) {
        this.id = id;
        this.type = type;
        this.score = score;
        this.source = source;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    public BytesReference getSource() {
        return source;
    }

    public void setSource(BytesReference source) {
        this.source = source;
    }

    @SuppressWarnings({"unchecked"})
    public Map<String, Object> sourceAsMap() throws ElasticsearchParseException {
        if (source == null) {
            return null;
        }
        if (sourceAsMap != null) {
            return sourceAsMap;
        }

        sourceAsMap = SourceLookup.sourceAsMap(source);
        return sourceAsMap;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.id = in.readString();
        this.type = in.readString();
        this.score = in.readFloat();

        this.source = in.readBytesReference();
        if (this.source.length() == 0) {
            this.source = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.id);
        out.writeString(this.type);
        out.writeFloat(this.score);

        // write source
        out.writeBytesReference(source);
    }

    @Override
    protected void buildXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TYPE, this.type);
        builder.field(Fields.ID, this.id);
        builder.field(Fields.SCORE, this.score);

        for (Map.Entry<String, Object> entry : this.sourceAsMap().entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
    }

    private static final class Fields {
        static final String TYPE = "_type";
        static final String ID = "_id";
        static final String SCORE = "_score";
    }
}
