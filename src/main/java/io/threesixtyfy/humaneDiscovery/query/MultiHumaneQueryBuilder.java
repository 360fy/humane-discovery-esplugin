package io.threesixtyfy.humaneDiscovery.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class MultiHumaneQueryBuilder extends BaseHumaneQueryBuilder<MultiHumaneQueryBuilder> {
    public static final String NAME = "multi_humane_query";

    private static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField PATH_FIELD = new ParseField("path");

    private QueryField[] queryFields;

    private MultiHumaneQueryBuilder() {
        super();
    }

    public MultiHumaneQueryBuilder(StreamInput in) throws IOException {
        super(in);

        int size = in.readInt();
        this.queryFields = new QueryField[size];

        for (int i = 0; i < size; i++) {
            this.queryFields[i] = new QueryField(in);
        }
    }

    public static Optional<MultiHumaneQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token;
        String currentFieldName = null;

        List<QueryField> queryFields = null;

        float boost = DEFAULT_BOOST;
        String queryName = null;
        String queryText = null;

        String intentIndex = null;
        Set<String> intentFields = new HashSet<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.getParseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                if (token.isValue()) {
                    queryText = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[query must be text]");
                }
            } else if (parseContext.getParseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
                if (token == XContentParser.Token.START_ARRAY) {
                    // parse fields
                    queryFields = parseQueryFields(parser, parseContext);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] query does not support [" + token + "]");
                }
            } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                queryName = parser.text();
            } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                boost = parser.floatValue();
            } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INTENT_INDEX_FIELD)) {
                intentIndex = parser.text();
            } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INTENT_FIELDS_FIELD)) {
                BaseHumaneQueryBuilder.parseIntentFields(parser, token, intentFields);
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] query does not support [" + currentFieldName + "]");
            }
        }

        if (queryText == null) {
            throw new ParsingException(parser.getTokenLocation(), "No text specified for text query");
        }

        if (queryFields == null || queryFields.size() == 0) {
            throw new ParsingException(parser.getTokenLocation(), "No fields specified for [" + NAME + "]");
        }

        if (queryFields.size() == 1) {
            throw new ParsingException(parser.getTokenLocation(), "For single field query use [humane_query] instead");
        }

        MultiHumaneQueryBuilder queryBuilder = new MultiHumaneQueryBuilder();
        queryBuilder.queryName = queryName;
        queryBuilder.boost = boost;
        queryBuilder.queryText = queryText;
        queryBuilder.queryFields = queryFields.toArray(new QueryField[queryFields.size()]);
        queryBuilder.intentIndex = intentIndex;
        queryBuilder.intentFields = intentFields;

        return Optional.of(queryBuilder);
    }

    private static List<QueryField> parseQueryFields(XContentParser parser, QueryParseContext parseContext) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        QueryField previousField = null;

        List<QueryField> queryFields = new LinkedList<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            QueryField queryField = new QueryField();

            if (token.isValue()) {
                // we have simple field
                queryField.name = parser.text();
            } else if (token == XContentParser.Token.START_OBJECT) {
                // we have object
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (parseContext.getParseFieldMatcher().match(currentFieldName, FIELD_FIELD)) {
                        queryField.name = parser.text();
                    } else if (parseContext.getParseFieldMatcher().match(currentFieldName, PATH_FIELD)) {
                        queryField.path = parser.text();
                    } else if (parseContext.getParseFieldMatcher().match(currentFieldName, NO_FUZZY_FIELD)) {
                        queryField.noFuzzy = parser.booleanValue();
                    } else if (parseContext.getParseFieldMatcher().match(currentFieldName, VERNACULAR_ONLY_FIELD)) {
                        queryField.vernacularOnly = parser.booleanValue();
                    } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                        queryField.boost = parser.floatValue();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] query does not support [" + currentFieldName + "]");
                    }
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] query does not support [" + token + "] for fields");
            }

            if (queryField.name == null) {
                throw new ParsingException(parser.getTokenLocation(), "field name not defined for field: " + queryFields.size());
            }

            if (queryField.boost == 1.0f && previousField != null) {
                queryField.boost = previousField.boost + 1.0f;
            }

            queryFields.add(queryField);
            previousField = queryField;
        }

        return queryFields;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);

        if (this.queryFields != null) {
            out.writeInt(this.queryFields.length);

            for (QueryField queryField : this.queryFields) {
                queryField.writeTo(out);
            }
        }
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.field(QUERY_FIELD.getPreferredName(), this.queryText);
        builder.field(INTENT_INDEX_FIELD.getPreferredName(), this.intentIndex);
        builder.array(INTENT_FIELDS_FIELD.getPreferredName(), this.intentFields);

        builder.startArray(FIELDS_FIELD.getPreferredName());

        for (QueryField queryField : queryFields) {
            builder.field(FIELD_FIELD.getPreferredName(), queryField.name);
            builder.field(PATH_FIELD.getPreferredName(), queryField.path);
            builder.field(NO_FUZZY_FIELD.getPreferredName(), queryField.noFuzzy);
            builder.field(VERNACULAR_ONLY_FIELD.getPreferredName(), queryField.vernacularOnly);
            builder.field(BOOST_FIELD.getPreferredName(), queryField.boost);
        }

        builder.endArray();

        printBoostAndQueryName(builder);

        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query query = parse(context, context.getClient(), this.queryFields);
        if (query == null) {
            return matchNoDocsQuery();
        }

        return query;
    }

    @Override
    protected boolean doEquals(MultiHumaneQueryBuilder other) {
        return super.checkEquals(other) && Arrays.equals(this.queryFields, other.queryFields);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.doHashCode(), Arrays.hashCode(this.queryFields));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
