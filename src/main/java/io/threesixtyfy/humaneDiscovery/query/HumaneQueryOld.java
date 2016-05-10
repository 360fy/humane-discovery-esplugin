package io.threesixtyfy.humaneDiscovery.query;

public class HumaneQueryOld /*extends Query*/ {
//    private final ESLogger logger = Loggers.getLogger(DidYouMeanQueryParser.class);
//
//    private static final String[] PhoneticFields = {"edgeGram", "phonetic_soundex", "phonetic_dm", "phonetic_bm", "phonetic_edgeGram_soundex", "phonetic_edgeGram_dm", "phonetic_edgeGram_bm"};
//    private static final String[] PhoneticSingles = {"phonetic_soundex", "phonetic_dm", "phonetic_bm"};
//
//    protected final Map<String, QueryBuilder> queryBuilderCache = new HashMap<>();
//
//    protected final QueryParseContext parseContext;
//
//    private final HashMap<String, String> synonyms = new HashMap<>();
//
//    public HumaneQueryOld(QueryParseContext parseContext) {
//        this.parseContext = parseContext;
//
//        synonyms.put("ibps", "institute of banking personnel selection");
//        synonyms.put("sbi", "state bank of india");
//        synonyms.put("rrb", "regional rural bank");
//        synonyms.put("rrb", "railway recruitment board");
//        synonyms.put("upsc", "union public service commission");
//        synonyms.put("ssc", "staff service commission");
//        synonyms.put("cgl", "combined graduate level examination");
//        synonyms.put("rbi", "reserve bank of india");
//        synonyms.put("sebi", "securities and exchange board of india");
//        synonyms.put("nabard", "national bank for agriculture and rural development");
//        synonyms.put("sidbi", "small industries development bank of india");
//        synonyms.put("sbh", "state bank of hyderabad");
//        synonyms.put("idbi", "industrial development bank of india");
//        synonyms.put("bob", "bank of baroda");
//        synonyms.put("ricem", "rajasthan institute of cooperative education and management");
//        synonyms.put("lvb", "laxmi vilas bank");
//        synonyms.put("apcob", "andhra pradesh state cooperative bank");
//        synonyms.put("dccb", "district central cooperative bank");
//        synonyms.put("hpscb", "hp state cooperative bank assistant manager");
//        synonyms.put("oscb", "odisha state cooperative bank");
//        synonyms.put("iob", "indian overseas bank");
//        synonyms.put("kvb", "karur vysya bank");
//        synonyms.put("svc", "shamrao vithal cooperative bank");
//        synonyms.put("cso", "customer service officer");
//        synonyms.put("csr", "customer service representative");
//        synonyms.put("oicl", "oriental insurance company limited");
//        synonyms.put("aic", "agriculture insurance company of india limited");
//        synonyms.put("lic", "life insurance corporation of india");
//        synonyms.put("aao", "assistant administrative officer");
//        synonyms.put("nia", "new india assurance");
//        synonyms.put("uii", "united india insurance");
//        synonyms.put("nicl", "national insurance company ltd");
//        synonyms.put("ado", "apprentice development officer");
//        synonyms.put("gic", "general insurance corporation");
//        synonyms.put("cmpfo", "coal mines provident fund organisation");
//        synonyms.put("ib", "intelligence bureau");
//        synonyms.put("acio", "assistant central intelligence officer");
//        synonyms.put("uii", "united india insurance");
//        synonyms.put("ubi", "union bank of india");
//        synonyms.put("fci", "food corporation of india");
//        synonyms.put("chsl", "combined higher secondary level");
//        synonyms.put("ldc", "lower division clerk");
//        synonyms.put("epfo", "the employees provident fund organisation");
//        synonyms.put("uptet", "uttar pradesh teachers eligibility test");
//        synonyms.put("si", "subinspector");
//        synonyms.put("rpsc", "rajasthan public service commission");
//        synonyms.put("ras", "rajasthan administrative service");
//        synonyms.put("ntpc", "non technical popular categories");
//        synonyms.put("ecrc", "enquiry cum reservation clerk");
//        synonyms.put("jaa", "junior accounts assistant cum typist");
//        synonyms.put("pgdbf", "post graduate diploma in banking and finance");
//        synonyms.put("ctet", "central teacher eligibility test");
//        synonyms.put("cds", "combined defence services");
//        synonyms.put("esic", "employees' state insurance corporation");
//        synonyms.put("udc", "upper division clerk");
//        synonyms.put("mts", "multi tasking staff");
//        synonyms.put("tspsc", "telangana state public service commission");
//        synonyms.put("irda", "insurance regulatory and development authority");
//        synonyms.put("ssc asi", "assistant subinspector ");
//        synonyms.put("isro", "indian space research organisation");
//        synonyms.put("dmrc", "delhi metro rail corporation");
//        synonyms.put("institute of banking personnel selection", "ibps");
//        synonyms.put("state bank of india", "sbi");
//        synonyms.put("regional rural bank", "rrb");
//        synonyms.put("railway recruitment board", "rrb");
//        synonyms.put("union public service commission", "upsc");
//        synonyms.put("staff service commission", "ssc");
//        synonyms.put("combined graduate level examination", "cgl");
//        synonyms.put("reserve bank of india", "rbi");
//        synonyms.put("securities and exchange board of india", "sebi");
//        synonyms.put("national bank for agriculture and rural development", "nabard");
//        synonyms.put("small industries development bank of india", "sidbi");
//        synonyms.put("state bank of hyderabad", "sbh");
//        synonyms.put("industrial development bank of india", "idbi");
//        synonyms.put("bank of baroda", "bob");
//        synonyms.put("rajasthan institute of cooperative education and management", "ricem");
//        synonyms.put("laxmi vilas bank", "lvb");
//        synonyms.put("andhra pradesh state cooperative bank", "apcob");
//        synonyms.put("district central cooperative bank", "dccb");
//        synonyms.put("hp state cooperative bank assistant manager", "hpscb");
//        synonyms.put("odisha state cooperative bank", "oscb");
//        synonyms.put("indian overseas bank", "iob");
//        synonyms.put("karur vysya bank", "kvb");
//        synonyms.put("shamrao vithal cooperative bank", "svc");
//        synonyms.put("customer service officer", "cso");
//        synonyms.put("customer service representative", "csr");
//        synonyms.put("oriental insurance company limited", "oicl");
//        synonyms.put("agriculture insurance company of india limited", "aic");
//        synonyms.put("life insurance corporation of india", "lic");
//        synonyms.put("assistant administrative officer", "aao");
//        synonyms.put("new india assurance", "nia");
//        synonyms.put("united india insurance", "uii");
//        synonyms.put("national insurance company ltd ", "nicl");
//        synonyms.put("apprentice development officer ", "ado");
//        synonyms.put("general insurance corporation", "gic");
//        synonyms.put("coal mines provident fund organisation", "cmpfo");
//        synonyms.put("intelligence bureau", "ib");
//        synonyms.put("assistant central intelligence officer", "acio");
//        synonyms.put("united india insurance", "uii");
//        synonyms.put("union bank of india", "ubi");
//        synonyms.put("food corporation of india", "fci");
//        synonyms.put("combined higher secondary level", "chsl");
//    }
//
//    public Query parse(String fieldName, Object value) throws IOException {
//        MappedFieldType fieldType = this.getFieldType(fieldName);
//
//        /*
//         * If the user forced an analyzer we really don't care if they are
//         * searching a type that wants term queries to be used with query string
//         * because the QueryBuilder will take care of it. If they haven't forced
//         * an analyzer then types like NumberFieldType that want terms with
//         * query string will blow up because their analyzer isn't capable of
//         * passing through QueryBuilder.
//         */
//        if (fieldType != null && fieldType.useTermQueryWithQueryString()) {
//            return termQuery(fieldType, value, true);
//        }
//
//        logger.info("Building humane query...");
//
//        try {
//            return humaneQuery(getFieldName(fieldName, fieldType), value.toString(), fieldType);
//        } catch (Throwable t) {
//            logger.error("Error in creating humane query", t);
//            throw t;
//        }
//    }
//
//    protected MappedFieldType getFieldType(String fieldName) {
//        return parseContext.fieldMapper(fieldName);
//    }
//
//    protected String getFieldName(String fieldName, MappedFieldType fieldType) {
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
//
//    protected Analyzer analyzer(MappedFieldType fieldType) {
//        if (fieldType != null) {
//            return parseContext.getSearchAnalyzer(fieldType);
//        }
//        return parseContext.mapperService().searchAnalyzer();
//    }
//
//    protected QueryBuilder queryBuilder(String fieldName, MappedFieldType fieldType) {
//        synchronized (queryBuilderCache) {
//            if (!queryBuilderCache.containsKey(fieldName)) {
//                Analyzer analyzer = this.analyzer(fieldType);
//                assert analyzer != null;
//
//                queryBuilderCache.put(fieldName, new QueryBuilder(analyzer));
//            }
//
//            return queryBuilderCache.get(fieldName);
//        }
//    }
//
//    protected QueryBuilder queryBuilder(String fieldName) {
//        synchronized (queryBuilderCache) {
//            if (!queryBuilderCache.containsKey(fieldName)) {
//                Analyzer analyzer = this.analyzer(getFieldType(fieldName));
//                assert analyzer != null;
//
//                queryBuilderCache.put(fieldName, new QueryBuilder(analyzer));
//            }
//
//            return queryBuilderCache.get(fieldName);
//        }
//    }
//
//    /**
//     * Creates a TermQuery-like-query for MappedFieldTypes that don't support
//     * QueryBuilder which is very string-ish. Just delegates to the
//     * MappedFieldType for MatchQuery but gets more complex for blended queries.
//     */
//    protected final Query termQuery(MappedFieldType fieldType, Object value, boolean lenient) {
//        try {
//            return fieldType.termQuery(value, parseContext);
//        } catch (RuntimeException e) {
//            if (lenient) {
//                return null;
//            }
//            throw e;
//        }
//    }
//
//    private Query buildQuery(String field, String text, boolean phrase) {
//        QueryBuilder queryBuilder = this.queryBuilder(field);
//        if (phrase) {
//            return queryBuilder.createPhraseQuery(field, text, 1);
//        } else {
//            return queryBuilder.createBooleanQuery(field, text);
//        }
//    }
//
//    private Collection<Query> buildFuzzyDisjuncts(Collection<Query> disjuncts, String field, String text, boolean phrase) {
//        disjuncts.add(buildQuery(field, text, phrase));
//
//        for (String phoneticField : PhoneticFields) {
//            disjuncts.add(buildQuery(field + "." + phoneticField, text, phrase));
//        }
//
//        return disjuncts;
//    }
//
//    private Collection<Query> buildFuzzyDisjuncts(String field, String text, boolean phrase) {
//        return this.buildFuzzyDisjuncts(new LinkedList<Query>(), field, text, phrase);
//    }
//
////    //    new ConstantScoreQuery(queryNode)
////    private Collection<Query> buildFuzzyDisjunctsIfTermQuery(Collection<Query> disjuncts, Query query, String[] fields, boolean phrase) {
////        if (!(query instanceof TermQuery)) {
////            disjuncts.add(query);
////            return disjuncts;
////        }
////
////        return this.buildFuzzyDisjuncts(disjuncts, ((TermQuery) query).getTerm().text(), fields, phrase);
////    }
//
//    private String joinArraySlice(String[] strArray, int start, int end) {
//        StringBuilder sb = new StringBuilder();
//
//        boolean first = true;
//        for (int i = start; i < end; i++) {
//            if (!first) {
//                sb.append(" ");
//            }
//
//            sb.append(strArray[i]);
//
//            first = false;
//        }
//
//        return sb.toString();
//    }
//
//    // for size 2 = bigram_shingles... and then combination with phonetic_soundex, phonetic_bm, phonetic_dm
//    // for size 3 = trigram_shingles... and then combination with phonetic_soundex, phonetic_bm, phonetic_dm
//    private Collection<Query> shingleQuery(String field, String text, int shingleSize) {
//        String shinglesType = (shingleSize == 2 ? "bigram" : "trigram") + "_shingles";
//
//        Collection<Query> disjuncts = new LinkedList<>();
//
//        disjuncts.add(this.buildQuery(field + "." + shinglesType, text, false));
//
//        for (String phoneticShingle : PhoneticSingles) {
//            disjuncts.add(this.buildQuery(field + "." + phoneticShingle + "_" + shinglesType, text, false));
//        }
//
//        logger.info("Shingle Disjuncts: {} for text: {}, field: {}, shingleSize: {}", disjuncts, text, field, shingleSize);
//
//        return disjuncts;
//    }
//
//    private Query buildGramsQuery(String field, int totalClauses, Query[] queryNodes, String[] queryTerms, int shift, int gramSize) {
//        if (shift >= totalClauses) {
//            return null;
//        }
//
//        BooleanQuery.Builder booleanQueryBuilder = null;
//
//        // todo: add synonyms expansion here
//        for (int i = shift; i < totalClauses; i += gramSize) {
//            boolean gramPossible = true;
//            for (int j = 0; j < gramSize; j++) {
//                if (i + j >= totalClauses || queryTerms[i + j] == null) {
//                    // we can not form gram of required size
//                    gramPossible = false;
//                }
//            }
//
//            if (!gramPossible) {
//                continue;
//            }
//
//            if (booleanQueryBuilder == null) {
//                booleanQueryBuilder = new BooleanQuery.Builder();
//            }
//
//            if (gramSize == 1) {
//                booleanQueryBuilder.add(queryNodes[i], BooleanClause.Occur.SHOULD);
//            } else {
//                BooleanQuery.Builder unigramQueryBuilder = new BooleanQuery.Builder();
//                for (int j = 0; j < gramSize; j++) {
//                    unigramQueryBuilder.add(queryNodes[i + j], BooleanClause.Occur.SHOULD);
//                }
//
//                String shingleText = joinArraySlice(queryTerms, i, i + gramSize);
//                Collection<Query> shingleDisjuncts = this.shingleQuery(field, shingleText, gramSize);
//
//                // dismax of shingle and unigrams
//                Collection<Query> disjuncts = new LinkedList<>();
//                disjuncts.addAll(shingleDisjuncts);
//                disjuncts.add(unigramQueryBuilder.build());
//                DisjunctionMaxQuery dismaxQuery = new DisjunctionMaxQuery(disjuncts, 1.0f);
//                booleanQueryBuilder.add(dismaxQuery, BooleanClause.Occur.SHOULD);
//            }
//        }
//
//        if (booleanQueryBuilder != null) {
//            return booleanQueryBuilder.build();
//        }
//
//        return null;
//    }
//
//    private void addNotNullDisjunct(Collection<Query> disjuncts, Query disjunct) {
//        if (disjunct != null) {
//            logger.info("Adding disjunct: {}", disjunct);
//            disjuncts.add(disjunct);
//        }
//    }
//
//    public Query humaneQuery(String field, String queryText, MappedFieldType fieldType) {
//        // todo: we normalize field weights to range of 10.
//        QueryBuilder queryBuilder = this.queryBuilder(field, fieldType);
//
//        Query booleanQuery = queryBuilder.createBooleanQuery(field, queryText);
//        logger.info("[DidYouMeanQuery] booleanQuery: {}", booleanQuery instanceof BooleanQuery);
//
//        // TODO: handle scenario when single term and not a boolean query
//        if (booleanQuery != null && booleanQuery instanceof BooleanQuery) {
//            BooleanQuery bq = (BooleanQuery) booleanQuery;
//
//            int numClauses = bq.clauses().size();
//
//            logger.info("[DidYouMeanQuery] numClauses: #{}", numClauses);
//
//            // query terms
//            Query[] queryNodes = new Query[numClauses];
//            String[] queryTerms = new String[numClauses];
//
//            int i = 0;
//            for (BooleanClause clause : bq.clauses()) {
//                queryNodes[i] = clause.getQuery();
//
//                if (clause.getQuery() instanceof TermQuery) {
//                    Term term = ((TermQuery) clause.getQuery()).getTerm();
//                    queryTerms[i] = term.text();
//                    queryNodes[i] = new DisjunctionMaxQuery(this.buildFuzzyDisjuncts(field, queryTerms[i], false), 1.0f);
//                }
//
//                i++;
//            }
//
//            // build disjunction of --
//            // a) shift 0 bigrams, eg for A B C => (AB | A+B) + C, similarly for A B C D => (AB | A+B) + (CD | C+D)
//            // b) shift 1 bigrams, eg for A B C => A + (BC | B+C), similarly for A B C D => A + (BC | B+C) + D
//            // c) shift 0 trigrams, eg for A B C => ABC, similarly for A B C D => ABC + D
//            // d) shift 1 trigrams, eg for A B C => nil, similarly for A B C D => A + BCD
//            // e) shift 2 trigrams, eg for A B C => nil, for A B C D => nil, for A B C D E => A + B + CDE
//            // e) unigrams
//
//            Collection<Query> gramDisjuncts = new LinkedList<>();
//
//            this.addNotNullDisjunct(gramDisjuncts, this.buildGramsQuery(field, numClauses, queryNodes, queryTerms, 0, 3));
//            this.addNotNullDisjunct(gramDisjuncts, this.buildGramsQuery(field, numClauses, queryNodes, queryTerms, 1, 3));
//            this.addNotNullDisjunct(gramDisjuncts, this.buildGramsQuery(field, numClauses, queryNodes, queryTerms, 2, 3));
//            this.addNotNullDisjunct(gramDisjuncts, this.buildGramsQuery(field, numClauses, queryNodes, queryTerms, 0, 2));
//            this.addNotNullDisjunct(gramDisjuncts, this.buildGramsQuery(field, numClauses, queryNodes, queryTerms, 1, 2));
//            this.addNotNullDisjunct(gramDisjuncts, this.buildGramsQuery(field, numClauses, queryNodes, queryTerms, 0, 1));
//
//            Query finalQuery = new DisjunctionMaxQuery(gramDisjuncts, 1.0f);
//
//            logger.info("Final Query: {}", finalQuery);
//
//            return finalQuery;
//
////            for (int j = 0; j < numClauses; j++) {
////                String term = queryTerms[j];
////                if (term == null) {
////                    continue;
////                }
////
////                // TODO: spelling correct the term if required... do we consider here synonyms too ?
////                // TODO: with corrected spellings finding synonyms would be combinatorial problem
////
////                boolean first = true;
////                StringBuilder sb = new StringBuilder();
////                for (int k = j; k >= 0; k--) {
////
////                    String kTerm = queryTerms[k];
////                    // ensure if there is null term in between we break
////                    if (kTerm == null) {
////                        break;
////                    }
////
////                    if (!first) {
////                        sb.insert(0, " ");
////                    }
////
////                    sb.insert(0, kTerm);
////
////                    first = false;
////
////                    // ensure when we include phrase... we include in its entirety
////                    Query queryNode = queryNodes[k];
////                    if (queryNode == null) {
////                        continue;
////                    }
////
////                    logger.info("[DidYouMeanQuery] looking for synonym: #{}", sb.toString());
////
////                    // check if there is a synonym for the running terms...
////                    String synonym = this.synonyms.get(sb.toString());
////                    if (synonym != null) {
////                        logger.info("[DidYouMeanQuery] got synonym: #{}", synonym);
////
////                        boolean multiWordSynonym = false;
////                        if (synonym.indexOf(' ') > 0) {
////                            multiWordSynonym = true;
////                        }
////
////                        // todo: pass lower boost to this, say 0.8
////                        Collection<Query> synonymDisjuncts = this.buildFuzzyDisjuncts(field, synonym, multiWordSynonym);
////
////                        if (k == j) {
////                            // if k == j, DisMax(kQueryNode, synonym term query or phrase query)... check num terms in synonym... replace queryNode at Kth position with this
////                            Collection<Query> disjunctionClauses = new LinkedList<>();
////
////                            if (queryNode instanceof DisjunctionMaxQuery) {
////                                disjunctionClauses.addAll(((DisjunctionMaxQuery) queryNode).getDisjuncts());
////                            } else {
////                                disjunctionClauses.add(queryNode); //todo: new ConstantScoreQuery(queryNode)
////                            }
////
////                            disjunctionClauses.addAll(synonymDisjuncts);
////
////                            queryNodes[k] = new DisjunctionMaxQuery(disjunctionClauses, 1.0f);
////                        } else {
////                            // else DisMax(Boolean(queryNodeK, ..., queryNodeJ), DisMax(Phrase query for sb, synonym term query or phrase query for synonym),
////                            // replace queryNode at Kth position with this new query and make queryNode(K+1), ..., queryNode(J) null.
////                            BooleanQuery.Builder kjQueryBuilder = new BooleanQuery.Builder();
////                            for (int l = k; l <= j; l++) {
////                                kjQueryBuilder.add(/*new ConstantScoreQuery(queryNodes[l])*/ queryNodes[l], BooleanClause.Occur.SHOULD);
////                            }
////
////                            Collection<Query> synonymDisjunctionClauses = new LinkedList<>();
//////                            synonymDisjunctionClauses.add(new BoostQuery(new ConstantScoreQuery(phraseQuery), j - k + 1));
//////                            synonymDisjunctionClauses.add(new BoostQuery(new ConstantScoreQuery(synonymDisjuncts), (j - k + 1) * 0.8f));
////                            this.buildFuzzyDisjuncts(synonymDisjunctionClauses, field, sb.toString(), true); // todo: add phrase weight
////                            synonymDisjunctionClauses.addAll(synonymDisjuncts); // todo: add synonym weight
////
////                            Query synonymQueryNode = new DisjunctionMaxQuery(synonymDisjunctionClauses, 1.0f);
////
////                            Collection<Query> finalDisjunctionClauses = new LinkedList<>();
////                            finalDisjunctionClauses.add(kjQueryBuilder.build());
////                            finalDisjunctionClauses.add(synonymQueryNode);
////
////                            queryNodes[k] = new DisjunctionMaxQuery(finalDisjunctionClauses, 1.0f);
////
////                            for (int l = k + 1; l <= j; l++) {
////                                queryNodes[l] = null;
////                            }
////                        }
////                    }
////                }
////            }
//
////            int numQueryNodes = 0;
////            Query lastNotNullQueryNode = null;
////            for (int j = 0; j < numClauses; j++) {
////                if (queryNodes[j] != null) {
////                    lastNotNullQueryNode = queryNodes[j];
////                    numQueryNodes++;
////                }
////            }
////
////            logger.info("[DidYouMeanQuery] numQueryNodes: #{}", numQueryNodes);
////
////            if (numQueryNodes <= 1) {
////                return lastNotNullQueryNode;
////            }
////
////            BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
////            for (int j = 0; j < numClauses; j++) {
////                if (queryNodes[j] != null) {
////                    booleanQueryBuilder.add(queryNodes[j], BooleanClause.Occur.SHOULD);
////                }
////            }
////
////            return booleanQueryBuilder.build();
//        }
//
//        return booleanQuery;
//    }
//
//    @Override
//    public String toString(String field) {
//        return getClass().getSimpleName();
//    }

}
