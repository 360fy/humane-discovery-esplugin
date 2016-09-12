package io.threesixtyfy.humaneDiscovery.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HumaneQueryParser implements QueryParser {

    public static final String TAG_QUERY = "query";
    public static final String TAG_BOOST = "boost";
    public static final String TAG_NO_FUZZY = "noFuzzy";
    public static final String TAG_NAME = "_name";
    public static final String TAG_VERNACULAR_ONLY = "vernacularOnly";
    public static final String TAG_INTENT_INDEX = "intentIndex";
    public static final String TAG_INTENT_FIELDS = "intentFields";

    private final ESLogger logger = Loggers.getLogger(HumaneQueryParser.class);

    public static final String HUMANE_QUERY = "humane_query";
    public static final String HumaneQuery = "humaneQuery";

    private final Client client;

    @Inject
    public HumaneQueryParser(Client client) {
        this.client = client;
    }

    @Override
    public String[] names() {
        return new String[]{HUMANE_QUERY, HumaneQuery};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        long startTime = System.currentTimeMillis();

        XContentParser parser = parseContext.parser();

        HumaneQuery humaneQuery = new HumaneQuery(parseContext);

        XContentParser.Token token;
        String currentFieldName = null;

        Object queryText = null;
        String queryName = null;

        String intentIndex = null;
        String[] intentFields = null;

        QueryField queryField = new QueryField();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                if (queryField.name != null) {
                    throw new QueryParsingException(parseContext, "[" + HUMANE_QUERY + "] query does not multi fields [" + currentFieldName + "], already seen [" + queryField.name + "]");
                }

                queryField.name = currentFieldName;

                if (token.isValue()) {
                    queryText = parser.objectText();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (TAG_QUERY.equals(currentFieldName)) {
                            queryText = parser.objectText();
                        } else if (TAG_BOOST.equals(currentFieldName)) {
                            queryField.boost = parser.floatValue();
                        } else if (TAG_NO_FUZZY.equals(currentFieldName)) {
                            queryField.noFuzzy = parser.booleanValue();
                        } else if (TAG_NAME.equals(currentFieldName)) {
                            queryName = parser.text();
                        } else if (TAG_VERNACULAR_ONLY.equals(currentFieldName)) {
                            queryField.vernacularOnly = parser.booleanValue();
                        } else if (TAG_INTENT_INDEX.equals(currentFieldName)) {
                            intentIndex = parser.text();
                        } else if (TAG_INTENT_FIELDS.equals(currentFieldName)) {
                            if (token == XContentParser.Token.START_ARRAY) {
                                // parse fields
                                List<String> intentFieldList = new ArrayList<>();

                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    if (token.isValue()) {
                                        // we have simple field
                                        intentFieldList.add(parser.text());
                                    } else {
                                        throw new QueryParsingException(parseContext, "intent field must be simple field name but got [" + token + "]");
                                    }
                                }

                                intentFields = intentFieldList.toArray(new String[intentFieldList.size()]);
                            } else {
                                throw new QueryParsingException(parseContext, "intentFields must be array of fields, but got [" + token + "]");
                            }
                        } else {
                            throw new QueryParsingException(parseContext, "[" + HUMANE_QUERY + "] query does not support [" + currentFieldName + "]");
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext, "[" + HUMANE_QUERY + "] query does not support [" + token + "]");
                }
            }
        }

        if (queryField.name == null) {
            throw new QueryParsingException(parseContext, "No field specified for query");
        }

        if (queryText == null) {
            throw new QueryParsingException(parseContext, "No text specified for query");
        }

        Query query = humaneQuery.parse(this.client, queryField, queryText, intentIndex, intentFields);
        if (query == null) {
            return Queries.newMatchNoDocsQuery();
        }

//        if (queryField.boost != DEFAULT_BOOST) {
//            query = new BoostQuery(query, queryField.boost);
//        }

        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("For queryText: {} and index: {} built query: {} in {}ms", queryText, parseContext.index().name(), query, (System.currentTimeMillis() - startTime));
        }

        return query;
    }

}
