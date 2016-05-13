package io.threesixtyfy.humaneDiscovery.query;

import io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionsBuilder;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class MultiFieldHumaneQueryParser implements QueryParser {

    private final ESLogger logger = Loggers.getLogger(MultiFieldHumaneQueryParser.class);

    public static final String NAME = "multi_humane_query";

    private SuggestionsBuilder suggestionsBuilder;

    @Override
    public String[] names() {
        return new String[]{NAME, "multiHumaneQuery"};
    }

    @Inject
    public void setSuggestionsBuilder(SuggestionsBuilder suggestionsBuilder) {
        this.suggestionsBuilder = suggestionsBuilder;
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        HumaneQuery humaneQuery = new HumaneQuery(parseContext);

        XContentParser.Token token;
        String currentFieldName = null;

        List<QueryField> queryFields = null;

        float boost = 1.0f;
        String queryName = null;
        Object queryText = null;

        QueryField previousField = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("query".equals(currentFieldName)) {
                if (token.isValue()) {
                    queryText = parser.objectText();
                } else {
                    throw new QueryParsingException(parseContext, "[query must be text]");
                }
            } else if ("fields".equals(currentFieldName)) {
                if (token == XContentParser.Token.START_ARRAY) {
                    // parse fields
                    queryFields = new LinkedList<>();

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
                                } else if ("field".equals(currentFieldName)) {
                                    queryField.name = parser.text();
                                } else if ("path".equals(currentFieldName)) {
                                    queryField.path = parser.text();
                                } else if ("noFuzzy".equals(currentFieldName)) {
                                    queryField.noFuzzy = parser.booleanValue();
                                } else if ("vernacularOnly".equals(currentFieldName)) {
                                    queryField.vernacularOnly = parser.booleanValue();
                                } else if ("boost".equals(currentFieldName)) {
                                    queryField.boost = parser.floatValue();
                                } else {
                                    throw new QueryParsingException(parseContext, "[" + NAME + "] query does not support [" + currentFieldName + "]");
                                }
                            }
                        } else {
                            throw new QueryParsingException(parseContext, "[" + NAME + "] query does not support [" + token + "] for fields");
                        }

                        if (queryField.name == null) {
                            throw new QueryParsingException(parseContext, "field name not defined for field: " + queryFields.size());
                        }

                        if (queryField.boost == 1.0f && previousField != null) {
                            queryField.boost = previousField.boost + 1.0f;
                        }

                        queryFields.add(queryField);
                        previousField = queryField;
                    }
                } else {
                    throw new QueryParsingException(parseContext, "[" + NAME + "] query does not support [" + token + "]");
                }
            } else if ("_name".equals(currentFieldName)) {
                queryName = parser.text();
            } else if ("boost".equals(currentFieldName)) {
                boost = parser.floatValue();
            } else {
                throw new QueryParsingException(parseContext, "[" + NAME + "] query does not support [" + currentFieldName + "]");
            }
        }

        if (queryText == null) {
            throw new QueryParsingException(parseContext, "No text specified for text query");
        }

        if (queryFields == null || queryFields.size() == 0) {
            throw new QueryParsingException(parseContext, "No fields specified for [" + NAME + "]");
        }

        if (queryFields.size() == 1) {
            throw new QueryParsingException(parseContext, "For single field query use [humane_query] instead");
        }

        Query query = humaneQuery.parse(suggestionsBuilder, queryFields.toArray(new QueryField[queryFields.size()]), queryText);
        if (query == null) {
            return Queries.newMatchNoDocsQuery();
        }

        if (boost != 1.0f) {
            query = new BoostQuery(query, boost);
        }

        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }

//        logger.info("Query: {}", query);

        return query;
    }
}
