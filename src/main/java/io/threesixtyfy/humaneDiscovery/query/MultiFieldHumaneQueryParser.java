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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class MultiFieldHumaneQueryParser implements QueryParser {

    public static final String TAG_QUERY = "query";
    public static final String TAG_FIELDS = "fields";
    public static final String TAG_FIELD = "field";
    public static final String TAG_PATH = "path";
    public static final String TAG_NO_FUZZY = "noFuzzy";
    public static final String TAG_VERNACULAR_ONLY = "vernacularOnly";
    public static final String TAG_BOOST = "boost";
    public static final String TAG_NAME = "_name";
    public static final String TAG_INTENT_INDEX = "intentIndex";
    public static final String TAG_INTENT_FIELDS = "intentFields";
    public static final float DEFAULT_BOOST = 1.0f;
    private final ESLogger logger = Loggers.getLogger(MultiFieldHumaneQueryParser.class);

    public static final String MULTI_HUMANE_QUERY = "multi_humane_query";
    public static final String MultiHumaneQuery = "multiHumaneQuery";

    private final Client client;

    @Inject
    public MultiFieldHumaneQueryParser(Client client) {
        this.client = client;
    }

    @Override
    public String[] names() {
        return new String[]{MULTI_HUMANE_QUERY, MultiHumaneQuery};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        long startTime = System.currentTimeMillis();

        XContentParser parser = parseContext.parser();

        HumaneQuery humaneQuery = new HumaneQuery(parseContext);

        XContentParser.Token token;
        String currentFieldName = null;

        List<QueryField> queryFields = null;

        float boost = DEFAULT_BOOST;
        String queryName = null;
        Object queryText = null;

        String intentIndex = null;
        Set<String> intentFields = new HashSet<>();

        QueryField previousField = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (TAG_QUERY.equals(currentFieldName)) {
                if (token.isValue()) {
                    queryText = parser.objectText();
                } else {
                    throw new QueryParsingException(parseContext, "[query must be text]");
                }
            } else if (TAG_FIELDS.equals(currentFieldName)) {
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
                                } else if (TAG_FIELD.equals(currentFieldName)) {
                                    queryField.name = parser.text();
                                } else if (TAG_PATH.equals(currentFieldName)) {
                                    queryField.path = parser.text();
                                } else if (TAG_NO_FUZZY.equals(currentFieldName)) {
                                    queryField.noFuzzy = parser.booleanValue();
                                } else if (TAG_VERNACULAR_ONLY.equals(currentFieldName)) {
                                    queryField.vernacularOnly = parser.booleanValue();
                                } else if (TAG_BOOST.equals(currentFieldName)) {
                                    queryField.boost = parser.floatValue();
                                } else {
                                    throw new QueryParsingException(parseContext, "[" + MULTI_HUMANE_QUERY + "] query does not support [" + currentFieldName + "]");
                                }
                            }
                        } else {
                            throw new QueryParsingException(parseContext, "[" + MULTI_HUMANE_QUERY + "] query does not support [" + token + "] for fields");
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
                    throw new QueryParsingException(parseContext, "[" + MULTI_HUMANE_QUERY + "] query does not support [" + token + "]");
                }
            } else if (TAG_NAME.equals(currentFieldName)) {
                queryName = parser.text();
            } else if (TAG_BOOST.equals(currentFieldName)) {
                boost = parser.floatValue();
            } else if (TAG_INTENT_INDEX.equals(currentFieldName)) {
                intentIndex = parser.text();
            } else if (TAG_INTENT_FIELDS.equals(currentFieldName)) {
                if (token == XContentParser.Token.START_ARRAY) {
                    // parse fields
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token.isValue()) {
                            // we have simple field
                            intentFields.add(parser.text());
                        } else {
                            throw new QueryParsingException(parseContext, "intent field must be simple field name but got [" + token + "]");
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext, "intentFields must be array of fields, but got [" + token + "]");
                }
            } else {
                throw new QueryParsingException(parseContext, "[" + MULTI_HUMANE_QUERY + "] query does not support [" + currentFieldName + "]");
            }
        }

        if (queryText == null) {
            throw new QueryParsingException(parseContext, "No text specified for text query");
        }

        if (queryFields == null || queryFields.size() == 0) {
            throw new QueryParsingException(parseContext, "No fields specified for [" + MULTI_HUMANE_QUERY + "]");
        }

        if (queryFields.size() == 1) {
            throw new QueryParsingException(parseContext, "For single field query use [humane_query] instead");
        }

        Query query = humaneQuery.parse(this.client, queryFields.toArray(new QueryField[queryFields.size()]), queryText, intentIndex, intentFields);
        if (query == null) {
            return Queries.newMatchNoDocsQuery();
        }

//        if (boost != DEFAULT_BOOST) {
//            query = new BoostQuery(query, boost);
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
