package io.threesixtyfy.humaneDiscovery.query;

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;

public class HumaneQueryParser implements QueryParser {

//    private final ESLogger logger = Loggers.getLogger(HumaneQueryParser.class);

    public static final String NAME = "humane_query";

    @Override
    public String[] names() {
        return new String[]{NAME, "humaneQuery"};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        HumaneQuery humaneQuery = new HumaneQuery(parseContext);

        XContentParser.Token token;
        String currentFieldName = null;

        Object queryText = null;
        String queryName = null;

        QueryField queryField = new QueryField();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                if (queryField.name != null) {
                    throw new QueryParsingException(parseContext, "[" + NAME + "] query does not multi fields [" + currentFieldName + "], already seen [" + queryField.name + "]");
                }

                queryField.name = currentFieldName;

                if (token.isValue()) {
                    queryText = parser.objectText();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if ("query".equals(currentFieldName)) {
                            queryText = parser.objectText();
                        } else if ("boost".equals(currentFieldName)) {
                            queryField.boost = parser.floatValue();
                        } else if ("noFuzzy".equals(currentFieldName)) {
                            queryField.noFuzzy = parser.booleanValue();
                        } else if ("_name".equals(currentFieldName)) {
                            queryName = parser.text();
                        } else if ("vernacularOnly".equals(currentFieldName)) {
                            queryField.vernacularOnly = parser.booleanValue();
                        } else {
                            throw new QueryParsingException(parseContext, "[" + NAME + "] query does not support [" + currentFieldName + "]");
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext, "[" + NAME + "] query does not support [" + token + "]");
                }
            }
        }

        if (queryField.name == null) {
            throw new QueryParsingException(parseContext, "No field specified for query");
        }

        if (queryText == null) {
            throw new QueryParsingException(parseContext, "No text specified for query");
        }

        Query query = humaneQuery.parse(queryField, queryText);
        if (query == null) {
            return null;
        }

        if (queryField.boost != 1.0f) {
            query = new BoostQuery(query, queryField.boost);
        }

        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }

        return query;
    }

}
