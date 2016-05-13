package io.threesixtyfy.humaneDiscovery.query;

import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Conjunct;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Disjunct;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.DisjunctsBuilder;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Suggestion;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionsBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HumaneQuery extends Query {
    private final ESLogger logger = Loggers.getLogger(HumaneQueryParser.class);

    private static final String StandardQueryAnalyzerName = "humane_query_analyzer";
    private static final String StandardEdgeGramQueryAnalyzerName = "humane_edgeGram_query_analyzer";
//
//    private static final String[] PhoneticAnalyzerNames = {
//            "phonetic_refined_soundex_search_analyzer",
//            "phonetic_dm_soundex_search_analyzer",
//            "phonetic_bm_search_analyzer",
//            "phonetic_dm_search_analyzer"
//    };
//    private static final String[] PhoneticEdgeGramAnalyzerNames = {
//            "phonetic_refined_soundex_edgeGram_search_analyzer",
//            "phonetic_dm_soundex_edgeGram_search_analyzer",
//            "phonetic_bm_edgeGram_search_analyzer",
//            "phonetic_dm_edgeGram_search_analyzer"
//    };

    private static final Map<String, QueryBuilder> QueryBuilderCache = new HashMap<>();

    private static final Map<String, NestedPathContext> NestedPathContextCache = new HashMap<>();

    protected final QueryParseContext parseContext;

    private final DisjunctsBuilder disjunctsBuilder = new DisjunctsBuilder();

//    private static final HashMap<String, String> Synonyms = new HashMap<>();
//
//    static {
//        Synonyms.put("ibps", "institute of banking personnel selection");
//        Synonyms.put("sbi", "state bank of india");
//        Synonyms.put("rrb", "regional rural bank");
//        Synonyms.put("rrb", "railway recruitment board");
//        Synonyms.put("upsc", "union public service commission");
//        Synonyms.put("ssc", "staff service commission");
//        Synonyms.put("cgl", "combined graduate level examination");
//        Synonyms.put("rbi", "reserve bank of india");
//        Synonyms.put("sebi", "securities and exchange board of india");
//        Synonyms.put("nabard", "national bank for agriculture and rural development");
//        Synonyms.put("sidbi", "small industries development bank of india");
//        Synonyms.put("sbh", "state bank of hyderabad");
//        Synonyms.put("idbi", "industrial development bank of india");
//        Synonyms.put("bob", "bank of baroda");
//        Synonyms.put("ricem", "rajasthan institute of cooperative education and management");
//        Synonyms.put("lvb", "laxmi vilas bank");
//        Synonyms.put("apcob", "andhra pradesh state cooperative bank");
//        Synonyms.put("dccb", "district central cooperative bank");
//        Synonyms.put("hpscb", "hp state cooperative bank assistant manager");
//        Synonyms.put("oscb", "odisha state cooperative bank");
//        Synonyms.put("iob", "indian overseas bank");
//        Synonyms.put("kvb", "karur vysya bank");
//        Synonyms.put("svc", "shamrao vithal cooperative bank");
//        Synonyms.put("cso", "customer service officer");
//        Synonyms.put("csr", "customer service representative");
//        Synonyms.put("oicl", "oriental insurance company limited");
//        Synonyms.put("aic", "agriculture insurance company of india limited");
//        Synonyms.put("lic", "life insurance corporation of india");
//        Synonyms.put("aao", "assistant administrative officer");
//        Synonyms.put("nia", "new india assurance");
//        Synonyms.put("uii", "united india insurance");
//        Synonyms.put("nicl", "national insurance company ltd");
//        Synonyms.put("ado", "apprentice development officer");
//        Synonyms.put("gic", "general insurance corporation");
//        Synonyms.put("cmpfo", "coal mines provident fund organisation");
//        Synonyms.put("ib", "intelligence bureau");
//        Synonyms.put("acio", "assistant central intelligence officer");
//        Synonyms.put("uii", "united india insurance");
//        Synonyms.put("ubi", "union bank of india");
//        Synonyms.put("fci", "food corporation of india");
//        Synonyms.put("chsl", "combined higher secondary level");
//        Synonyms.put("ldc", "lower division clerk");
//        Synonyms.put("epfo", "the employees provident fund organisation");
//        Synonyms.put("uptet", "uttar pradesh teachers eligibility test");
//        Synonyms.put("si", "subinspector");
//        Synonyms.put("rpsc", "rajasthan public service commission");
//        Synonyms.put("ras", "rajasthan administrative service");
//        Synonyms.put("ntpc", "non technical popular categories");
//        Synonyms.put("ecrc", "enquiry cum reservation clerk");
//        Synonyms.put("jaa", "junior accounts assistant cum typist");
//        Synonyms.put("pgdbf", "post graduate diploma in banking and finance");
//        Synonyms.put("ctet", "central teacher eligibility test");
//        Synonyms.put("cds", "combined defence services");
//        Synonyms.put("esic", "employees' state insurance corporation");
//        Synonyms.put("udc", "upper division clerk");
//        Synonyms.put("mts", "multi tasking staff");
//        Synonyms.put("tspsc", "telangana state public service commission");
//        Synonyms.put("irda", "insurance regulatory and development authority");
//        Synonyms.put("ssc asi", "assistant subinspector ");
//        Synonyms.put("isro", "indian space research organisation");
//        Synonyms.put("dmrc", "delhi metro rail corporation");
//        Synonyms.put("institute of banking personnel selection", "ibps");
//        Synonyms.put("state bank of india", "sbi");
//        Synonyms.put("regional rural bank", "rrb");
//        Synonyms.put("railway recruitment board", "rrb");
//        Synonyms.put("union public service commission", "upsc");
//        Synonyms.put("staff service commission", "ssc");
//        Synonyms.put("combined graduate level examination", "cgl");
//        Synonyms.put("reserve bank of india", "rbi");
//        Synonyms.put("securities and exchange board of india", "sebi");
//        Synonyms.put("national bank for agriculture and rural development", "nabard");
//        Synonyms.put("small industries development bank of india", "sidbi");
//        Synonyms.put("state bank of hyderabad", "sbh");
//        Synonyms.put("industrial development bank of india", "idbi");
//        Synonyms.put("bank of baroda", "bob");
//        Synonyms.put("rajasthan institute of cooperative education and management", "ricem");
//        Synonyms.put("laxmi vilas bank", "lvb");
//        Synonyms.put("andhra pradesh state cooperative bank", "apcob");
//        Synonyms.put("district central cooperative bank", "dccb");
//        Synonyms.put("hp state cooperative bank assistant manager", "hpscb");
//        Synonyms.put("odisha state cooperative bank", "oscb");
//        Synonyms.put("indian overseas bank", "iob");
//        Synonyms.put("karur vysya bank", "kvb");
//        Synonyms.put("shamrao vithal cooperative bank", "svc");
//        Synonyms.put("customer service officer", "cso");
//        Synonyms.put("customer service representative", "csr");
//        Synonyms.put("oriental insurance company limited", "oicl");
//        Synonyms.put("agriculture insurance company of india limited", "aic");
//        Synonyms.put("life insurance corporation of india", "lic");
//        Synonyms.put("assistant administrative officer", "aao");
//        Synonyms.put("new india assurance", "nia");
//        Synonyms.put("united india insurance", "uii");
//        Synonyms.put("national insurance company ltd ", "nicl");
//        Synonyms.put("apprentice development officer ", "ado");
//        Synonyms.put("general insurance corporation", "gic");
//        Synonyms.put("coal mines provident fund organisation", "cmpfo");
//        Synonyms.put("intelligence bureau", "ib");
//        Synonyms.put("assistant central intelligence officer", "acio");
//        Synonyms.put("united india insurance", "uii");
//        Synonyms.put("union bank of india", "ubi");
//        Synonyms.put("food corporation of india", "fci");
//        Synonyms.put("combined higher secondary level", "chsl");
//    }

    public HumaneQuery(QueryParseContext parseContext) {
        this.parseContext = parseContext;
    }

//    public Query parse(String fieldName, Object value) throws IOException {
//        try {
//            QueryField queryField = new QueryField();
//            queryField.name = fieldName;
//            queryField.boost = 1.0f;
//
//            QueryField[] queryFields = {queryField};
//
//            return humaneQuery(queryFields, value.toString());
//        } catch (Throwable t) {
//            logger.error("Error in creating humane query", t);
//            throw t;
//        }
//    }

    public Query parse(SuggestionsBuilder suggestionsBuilder, QueryField field, Object value) throws IOException {
        try {
            QueryField[] queryFields = {field};

            return humaneQuery(suggestionsBuilder, queryFields, value.toString());
        } catch (Throwable t) {
            logger.error("Error in creating humane query", t);
            throw t;
        }
    }

    public Query parse(SuggestionsBuilder suggestionsBuilder, QueryField[] fields, Object value) throws IOException {
        try {
            return humaneQuery(suggestionsBuilder, fields, value.toString());
        } catch (Throwable t) {
            logger.error("Error in creating humane query", t);
            throw t;
        }
    }

//    protected MappedFieldType getFieldType(String fieldName) {
//        return parseContext.fieldMapper(fieldName);
//    }
//
//    protected String getFieldName(String fieldName) {
//        MappedFieldType fieldType = getFieldType(fieldName);
//
//        final String field;
//
//        if (fieldType != null) {
//            field = fieldType.names().indexName();
//        } else {
//            field = fieldName;
//        }
//
//        return field;
//    }


    protected NestedPathContext nestedPathContext(String path) {
        synchronized (NestedPathContextCache) {
            if (!NestedPathContextCache.containsKey(path)) {
                NestedPathContext nestedPathContext = new NestedPathContext();
                nestedPathContext.path = path;

                ObjectMapper nestedObjectMapper = parseContext.getObjectMapper(path);

                if (nestedObjectMapper == null) {
                    throw new QueryParsingException(parseContext, "[nested] failed to find nested object under path [" + path + "]");
                }

                if (!nestedObjectMapper.nested().isNested()) {
                    throw new QueryParsingException(parseContext, "[nested] nested object under path [" + path + "] is not of nested type");
                }

                ObjectMapper objectMapper = parseContext.nestedScope().getObjectMapper();
                if (objectMapper == null) {
                    nestedPathContext.parentFilter = parseContext.bitsetFilter(Queries.newNonNestedFilter());
                } else {
                    nestedPathContext.parentFilter = parseContext.bitsetFilter(objectMapper.nestedTypeFilter());
                }

                nestedPathContext.childFilter = nestedObjectMapper.nestedTypeFilter();

                // nestedPathContext.parentObjectMapper = parseContext.nestedScope().nextLevel(nestedPathContext.nestedObjectMapper);

                // todo: for multiple hierarchy do above in recursive manner
                // reset level
                // parseContext.nestedScope().previousLevel();

                NestedPathContextCache.put(path, nestedPathContext);
            }

            return NestedPathContextCache.get(path);
        }
    }

    protected Analyzer analyzer(String analyzerName) {
        Analyzer analyzer = parseContext.mapperService().analysisService().analyzer(analyzerName);
        if (analyzer == null) {
            throw new IllegalArgumentException("No analyzer found for [" + analyzerName + "]");
        }

        return analyzer;
    }

    protected QueryBuilder queryBuilder(String analyzerName) {
        synchronized (QueryBuilderCache) {
            if (!QueryBuilderCache.containsKey(analyzerName)) {
                Analyzer analyzer = this.analyzer(analyzerName);
                assert analyzer != null;

                QueryBuilderCache.put(analyzerName, new QueryBuilder(analyzer));
            }

            return QueryBuilderCache.get(analyzerName);
        }
    }

    protected Query multiFieldQuery(QueryField[] queryFields, String text, boolean shingle, int numTokens, Set<Suggestion> suggestions) {
        if (queryFields.length == 1) {
            return this.fieldQuery(queryFields[0], text, shingle, numTokens, suggestions);
        }

        List<Query> fieldDisjuncts = new LinkedList<>();

        for (QueryField queryField : queryFields) {
            fieldDisjuncts.add(this.fieldQuery(queryField, text, shingle, numTokens, suggestions));
        }

        return new DisjunctionMaxQuery(fieldDisjuncts, 1.0f);
    }

    // exact = 50 % field weight
    // edgeGram = 30 % field weight
    // phonetic = 15 % field weight
    // edgeGramPhonetic = 5 % field weight
    protected Query fieldQuery(QueryField field, String text, boolean shingle, int numTokens, Set<Suggestion> suggestions) {
        BooleanQuery.Builder fieldQueryBuilder = new BooleanQuery.Builder();

        boolean noFuzzy = field.noFuzzy || numTokens == 1 && text.length() <= 3 || numTokens > 1 && text.length() <= 2;

//        if (!noFuzzy) {
//            fieldQueryBuilder.add(fieldFuzzyQuery(fieldFuzzyClauses(field, text, phrase, PhoneticAnalyzerNames), 0.15f * field.boost), BooleanClause.Occur.SHOULD);
//            fieldQueryBuilder.add(fieldFuzzyQuery(fieldFuzzyClauses(field, text, phrase, PhoneticEdgeGramAnalyzerNames), 0.05f * field.boost), BooleanClause.Occur.SHOULD);
//            fieldQueryBuilder.add(buildQuery(field, StandardQueryAnalyzerName, text, phrase, 0.50f * field.boost), BooleanClause.Occur.SHOULD);
//            fieldQueryBuilder.add(buildQuery(field, StandardEdgeGramQueryAnalyzerName, text, phrase, 0.30f * field.boost), BooleanClause.Occur.SHOULD);
//        } else {
//            fieldQueryBuilder.add(buildQuery(field, StandardQueryAnalyzerName, text, phrase, 0.75f * field.boost), BooleanClause.Occur.SHOULD);
//            fieldQueryBuilder.add(buildQuery(field, StandardEdgeGramQueryAnalyzerName, text, phrase, 0.25f * field.boost), BooleanClause.Occur.SHOULD);
//        }

        fieldQueryBuilder.add(buildQuery(field, StandardQueryAnalyzerName, text, shingle, 100.0f * field.boost), BooleanClause.Occur.SHOULD);
        fieldQueryBuilder.add(buildQuery(field, StandardEdgeGramQueryAnalyzerName, text, shingle, 10.0f * field.boost), BooleanClause.Occur.SHOULD);

        if (!noFuzzy && suggestions != null) {
            for (Suggestion suggestion : suggestions) {
                if (/*suggestion.getMatchLevel().getLevel() > MatchLevel.EdgeGram.getLevel()*/ !StringUtils.equals(suggestion.getSuggestion(), text)) {
                    fieldQueryBuilder.add(buildQuery(field, StandardQueryAnalyzerName, suggestion.getSuggestion(), shingle, (1 - suggestion.getEditDistance() / 10.0f) * field.boost), BooleanClause.Occur.SHOULD);
                }
            }
        }

        Query query = fieldQueryBuilder.build();

        // create nested path
        if (field.path != null) {
            NestedPathContext nestedPathContext = nestedPathContext(field.path);
            return new ToParentBlockJoinQuery(Queries.filtered(query, nestedPathContext.childFilter), nestedPathContext.parentFilter, ScoreMode.Avg);
        }

        return query;
    }

//    protected Query fieldFuzzyQuery(List<Query> phoneticClauses, float maxWeight) {
//        BooleanQuery.Builder phoneticQueryBuilder = new BooleanQuery.Builder();
//
//        float phoneticClauseWeight = maxWeight / phoneticClauses.size();
//
//        for (Query clause : phoneticClauses) {
//            phoneticQueryBuilder.add(constantScoreQuery(clause, phoneticClauseWeight), BooleanClause.Occur.SHOULD);
//        }
//
//        phoneticQueryBuilder.setMinimumNumberShouldMatch(2);
//
//        return phoneticQueryBuilder.build();
//    }
//
//    protected List<Query> fieldFuzzyClauses(QueryField field, String text, boolean phrase, String[] fuzzyAnalyzerNames) {
//        List<Query> fuzzyClauses = new LinkedList<>();
//        for (String analyzerName : fuzzyAnalyzerNames) { // noFuzzy ones
//            Query fuzzyQuery = this.buildQuery(field, analyzerName, text, phrase, 1.0f);
//            if (fuzzyQuery instanceof BooleanQuery) {
//                BooleanQuery bq = (BooleanQuery) fuzzyQuery;
//                for (BooleanClause clause : bq.clauses()) {
//                    fuzzyClauses.add(clause.getQuery());
//                }
//            } else {
//                fuzzyClauses.add(fuzzyQuery);
//            }
//        }
//
//        return fuzzyClauses;
//    }

    protected Query buildQuery(QueryField field, String analyzer, String text, boolean shingle, float weight) {
        QueryBuilder queryBuilder = this.queryBuilder(analyzer);

        String fieldName = field.name + (shingle ? ".shingle" : ".humane");

        Query query = queryBuilder.createBooleanQuery(fieldName, text);
        if (query instanceof TermQuery) {
            query = constantScoreQuery(query, weight);
        } else if (query instanceof BooleanQuery) {
            BooleanQuery bq = (BooleanQuery) query;
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (BooleanClause clause : bq.clauses()) {
                if (clause.getQuery() instanceof TermQuery) {
                    TermQuery termQuery = (TermQuery) clause.getQuery();
                    builder.add(constantScoreQuery(termQuery, weight), BooleanClause.Occur.SHOULD);
                } else {
                    builder.add(clause.getQuery(), BooleanClause.Occur.SHOULD);
                }
            }

            query = builder.build();
        }

        return query;
    }

    protected Query constantScoreQuery(Query query, float weight) {
        if (weight == 1.0f) {
            return new ConstantScoreQuery(query);
        } else {
            return new BoostQuery(new ConstantScoreQuery(query), weight);
        }
    }

//    private Collection<Query> fieldQuery(List<QueryField> queryFields, String text, boolean phrase) {
//        return this.fieldQuery(new LinkedList<Query>(), queryFields, text, phrase);
//    }

    public static String toString(BytesRef termText) {
        // the term might not be text, but usually is. so we make a best effort
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(ByteBuffer.wrap(termText.bytes, termText.offset, termText.length)).toString();
        } catch (CharacterCodingException e) {
            return termText.toString();
        }
    }

//    protected String[] tokens(String text) {
//        ArrayList<String> tokenList = new ArrayList<>();
//
//        Analyzer standardSearchAnalyzer = this.analyzer(StandardQueryAnalyzerName);
//
//        try (TokenStream source = standardSearchAnalyzer.tokenStream("DUMMY", text);
//             CachingTokenFilter stream = new CachingTokenFilter(source)) {
//            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
//
//            stream.reset();
//            while (stream.incrementToken()) {
//                String token = toString(termAtt.getBytesRef());
//                tokenList.add(token);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException("Error analyzing query text", e);
//        }
//
//        return tokenList.toArray(new String[tokenList.size()]);
//    }

    private Query query(Query[] queryNodes, int numTokens) {
        int numQueryNodes = 0;
        Query lastNotNullQueryNode = null;
        for (int j = 0; j < numTokens; j++) {
            if (queryNodes[j] != null) {
                lastNotNullQueryNode = queryNodes[j];
                numQueryNodes++;
            }
        }

        if (numQueryNodes <= 1) {
            return lastNotNullQueryNode;
        }

        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        for (int j = 0; j < numTokens; j++) {
            if (queryNodes[j] != null) {
                booleanQueryBuilder.add(queryNodes[j], BooleanClause.Occur.SHOULD);
            }
        }

        booleanQueryBuilder.setMinimumNumberShouldMatch(minimumShouldMatch(numTokens));

        return booleanQueryBuilder.build();
    }

    private int minimumShouldMatch(int numTokens) {
        if (numTokens <= 2) {
            return numTokens;
        } else if (numTokens <= 4) {
            return (int) Math.floor(0.90 * numTokens);
        } else if (numTokens <= 6) {
            return (int) Math.floor(0.80 * numTokens);
        } else {
            return (int) Math.floor(0.70 * numTokens);
        }
    }

    @SuppressWarnings("unchecked")
    protected Query humaneQuery(SuggestionsBuilder suggestionsBuilder, QueryField[] queryFields, String queryText) throws IOException {

        List<String> tokens = suggestionsBuilder.tokens(this.parseContext.analysisService(), queryText);

        int numTokens = tokens.size();

        if (numTokens == 0) {
            return null;
        }

        String indexName = this.parseContext.index().name();

        if (indexName.contains(":search_query_store")) {
            Query[] queryNodes = new Query[numTokens];

            for (int i = 0; i < numTokens; i++) {
                String token = tokens.get(i);
                queryNodes[i] = this.multiFieldQuery(queryFields, token, false, numTokens, null);
            }

            return query(queryNodes, numTokens);
        } else {
            Map<String, Conjunct> conjunctMap = new HashMap<>();
            Set<Disjunct> disjuncts = disjunctsBuilder.build(tokens, conjunctMap);

            final Map<String, Set<Suggestion>> suggestionsMap = suggestionsBuilder.fetchSuggestions(conjunctMap.values(), indexName + ":did_you_mean_store");
            if (suggestionsMap.size() == 0) {
                return null;
            }

//            logger.info("Suggestions Map: {}", suggestionsMap);
//            logger.info("Disjuncts: {}", disjuncts);

            List<Query> queries = new ArrayList<>();

            for (Disjunct disjunct : disjuncts) {
                // check if all conjuncts have suggestions ?

                int count = 0;
                BooleanQuery.Builder disjunctQueryBuilder = new BooleanQuery.Builder();
                for (Conjunct conjunct : disjunct.getConjuncts()) {
                    // depending on the conjunct length form a query
                    if (conjunct.getLength() == 1) {
                        // we form normal query
                        String token = conjunct.getTokens().get(0);
                        String key = token;
                        Set<Suggestion> suggestions = suggestionsMap.get(key);
                        Query termQuery = null;
                        if (suggestions != null) {
                            termQuery = this.multiFieldQuery(queryFields, token, false, numTokens, suggestions);
                        }

                        key = key + "/joined";
                        suggestions = suggestionsMap.get(key);
                        Query joinedQuery = null;
                        if (suggestions != null) {
                            // query in shingle field
                            joinedQuery = this.multiFieldQuery(queryFields, token, true, numTokens, suggestions);
//                            logger.info("Joined Query: {}", joinedQuery);
                        }

                        if (termQuery != null && joinedQuery != null) {
                            BooleanQuery.Builder builder = new BooleanQuery.Builder();
                            builder.add(termQuery, BooleanClause.Occur.SHOULD);
                            builder.add(joinedQuery, BooleanClause.Occur.SHOULD);
                            disjunctQueryBuilder.add(builder.build(), BooleanClause.Occur.SHOULD);
                            count++;
                        } else if (termQuery != null) {
                            disjunctQueryBuilder.add(termQuery, BooleanClause.Occur.SHOULD);
                            count++;
                        } else if (joinedQuery != null) {
                            disjunctQueryBuilder.add(joinedQuery, BooleanClause.Occur.SHOULD);
                            count++;
                        }
                    } else {
                        Query shingleQuery = null;
                        if (conjunct.getLength() == 2) {
                            // we form a shingle query
                            String shingleToken = StringUtils.join(conjunct.getTokens(), "");
                            String key = conjunct.getKey() + "/shingle";
                            Set<Suggestion> suggestions = suggestionsMap.get(key);

                            if (suggestions != null) {
                                shingleQuery = this.multiFieldQuery(queryFields, shingleToken, true, numTokens, suggestions);
                            }
                        }

                        // if compound token exists we form a compound query
                        String compoundToken = StringUtils.join(conjunct.getTokens());
                        String key = conjunct.getKey() + "/compound";
                        Set<Suggestion> suggestions = suggestionsMap.get(key);
                        Query compoundQuery = null;
                        if (suggestions != null) {
                            compoundQuery = this.multiFieldQuery(queryFields, compoundToken, false, numTokens, suggestions);
                        }

                        if (shingleQuery != null && compoundQuery != null) {
                            BooleanQuery.Builder builder = new BooleanQuery.Builder();
                            builder.add(shingleQuery, BooleanClause.Occur.SHOULD);
                            builder.add(compoundQuery, BooleanClause.Occur.SHOULD);
                            disjunctQueryBuilder.add(builder.build(), BooleanClause.Occur.MUST);
                        } else if (compoundQuery != null) {
                            disjunctQueryBuilder.add(compoundQuery, BooleanClause.Occur.MUST);
                        } else if (shingleQuery != null) {
                            disjunctQueryBuilder.add(shingleQuery, BooleanClause.Occur.MUST);
                        }
                    }
                }

                disjunctQueryBuilder.setMinimumNumberShouldMatch(minimumShouldMatch(count));

                queries.add(disjunctQueryBuilder.build());
            }

            if (queries.size() == 0) {
                return queries.get(0);
            }

            return new DisjunctionMaxQuery(queries, 1.0f);
        }
    }

    @Override
    public String toString(String field) {
        return getClass().getSimpleName();
    }

    static class NestedPathContext {
        String path;
        // ObjectMapper nestedObjectMapper;
        // ObjectMapper parentObjectMapper;
        BitSetProducer parentFilter;
        Query childFilter;
    }

}
