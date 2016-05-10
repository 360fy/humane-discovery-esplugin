package io.threesixtyfy.humaneDiscovery.query;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoostableQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

// TODO: build humane query
public class HumaneQueryBuilder extends QueryBuilder implements BoostableQueryBuilder<HumaneQueryBuilder> {
    private final String name;

    private final Object text;

    private Float boost;

    private String queryName;

    /**
     * Constructs a new text query.
     */
    public HumaneQueryBuilder(String name, Object text) {
        this.name = name;
        this.text = text;
    }

    /**
     * Set the boost to apply to the query.
     */
    @Override
    public HumaneQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public HumaneQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(HumaneQueryParser.NAME);
        builder.startObject(name);

        builder.field("query", text);
        if (boost != null) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }


        builder.endObject();
        builder.endObject();
    }
}
