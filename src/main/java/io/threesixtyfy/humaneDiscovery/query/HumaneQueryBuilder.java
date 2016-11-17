package io.threesixtyfy.humaneDiscovery.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class HumaneQueryBuilder extends BaseHumaneQueryBuilder<HumaneQueryBuilder> {
    public static final String NAME = "humane_query";

    private QueryField queryField;

    private HumaneQueryBuilder() {
        super();
    }

    public HumaneQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.queryField = new QueryField(in);
    }

    public static Optional<HumaneQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token;
        String currentFieldName = null;

        float boost = DEFAULT_BOOST;
        String queryText = null;
        String queryName = null;

        String intentIndex = null;
        Set<String> intentFields = new HashSet<>();

        QueryField queryField = new QueryField();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                if (queryField.name != null) {
                    throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] query does not support multi fields [" + currentFieldName + "], already seen [" + queryField.name + "]");
                }

                queryField.name = currentFieldName;

                if (token.isValue()) {
                    queryText = parser.text();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                            queryText = parser.text();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, BOOST_FIELD)) {
                            boost = queryField.boost = parser.floatValue();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, NO_FUZZY_FIELD)) {
                            queryField.noFuzzy = parser.booleanValue();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, NAME_FIELD)) {
                            queryName = parser.text();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, VERNACULAR_ONLY_FIELD)) {
                            queryField.vernacularOnly = parser.booleanValue();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INTENT_INDEX_FIELD)) {
                            intentIndex = parser.text();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INTENT_FIELDS_FIELD)) {
                            parseIntentFields(parser, token, intentFields);
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] query does not support [" + currentFieldName + "]");
                        }
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] query does not support [" + token + "]");
                }
            }
        }

        if (queryField.name == null) {
            throw new ElasticsearchException("No field specified for query");
        }

        if (queryText == null) {
            throw new ElasticsearchException("No text specified for query");
        }

        HumaneQueryBuilder queryBuilder = new HumaneQueryBuilder();
        queryBuilder.queryName = queryName;
        queryBuilder.boost = boost;
        queryBuilder.queryText = queryText;
        queryBuilder.queryField = queryField;
        queryBuilder.intentIndex = intentIndex;
        queryBuilder.intentFields = intentFields;

        return Optional.of(queryBuilder);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);

        if (this.queryField != null) {
            this.queryField.writeTo(out);
        }
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(this.queryField.name);

        builder.field(QUERY_FIELD.getPreferredName(), this.queryText);
        builder.field(NO_FUZZY_FIELD.getPreferredName(), this.queryField.noFuzzy);
        builder.field(VERNACULAR_ONLY_FIELD.getPreferredName(), this.queryField.vernacularOnly);
        builder.field(INTENT_INDEX_FIELD.getPreferredName(), this.intentIndex);
        builder.array(INTENT_FIELDS_FIELD.getPreferredName(), this.intentFields);

        printBoostAndQueryName(builder);

        builder.endObject();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query query = parse(context, context.getClient(), this.queryField);
        if (query == null) {
            return matchNoDocsQuery();
        }

        return query;
    }

    @Override
    protected boolean doEquals(HumaneQueryBuilder other) {
        return super.checkEquals(other) && Objects.equals(this.queryField, other.queryField);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.doHashCode(), this.queryField);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

}
