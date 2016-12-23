package io.threesixtyfy.humaneDiscovery.core.tagger;

import io.threesixtyfy.humaneDiscovery.core.encoding.EncodingsBuilder;
import io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.Fields.ENCODINGS;
import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.TOKEN_NESTED_FIELD;
import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.encodingNestedField;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.disMaxQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class TokenSetQueryBuilder {

    private static final Logger logger = Loggers.getLogger(TokenSetQueryBuilder.class);

    private static final float EXACT_TERM_BOOST = 1050.0f;
    private static final int MINIMUM_NUMBER_SHOULD_MATCH = 2;
//    private static final String SCORE_SCRIPT = "_score / doc['tokenCount'].value";

    private final TokenQueryBuilder tokenQueryBuilder = new TokenQueryBuilder();
    private final EncodingsBuilder encodingsBuilder = new EncodingsBuilder();

    public QueryBuilder buildQuery(Set<? extends TagScope> tagScopes, List<String> tokens) {
        // TODO: add scope

        BoolQueryBuilder boolQueryBuilder = boolQuery().minimumNumberShouldMatch(1)/*.disableCoord(true)*/;

        for (String token : tokens) {
            TokenWithEncodings tokenWithEncodings = getTokenWithEncodings(token);
            boolQueryBuilder.should(nestedQuery(ENCODINGS, tokenQueryBuilder.query(tokenWithEncodings), ScoreMode.Max));
        }

        // add query for joined token
        if (tokens.size() > 1) {
            String joinedToken = tokens.stream().collect(Collectors.joining());
            TokenWithEncodings tokenWithEncodings = getTokenWithEncodings(joinedToken);
            boolQueryBuilder.should(nestedQuery(ENCODINGS, tokenQueryBuilder.query(tokenWithEncodings), ScoreMode.Max));
        }

        QueryBuilder queryBuilder = simplifyBoolQuery(boolQueryBuilder);

//        QueryBuilder queryBuilder = functionScoreQuery(nestedQuery,
//                ScoreFunctionBuilders.scriptFunction(new Script(SCORE_SCRIPT,
//                        ScriptService.ScriptType.INLINE,
//                        "expression",
//                        null)));

        return queryBuilder;
    }

    private QueryBuilder simplifyBoolQuery(BoolQueryBuilder boolQueryBuilder) {
        if (boolQueryBuilder.should().size() == 1) {
            return boolQueryBuilder.should().get(0);
        }

        return boolQueryBuilder;
    }

//    private TokenWithEncodings[] getTokenWithEncodingsSet(List<String> tokens) {
//        TokenWithEncodings[] tokenWithEncodingsSet = new TokenWithEncodings[tokens.size()];
//
//        int i = 0;
//        for (String token : tokens) {
//            tokenWithEncodingsSet[i++] = getTokenWithEncodings(token);
//        }
//
//        return tokenWithEncodingsSet;
//    }

    private TokenWithEncodings getTokenWithEncodings(String token) {
        Map<String, Set<String>> encodings = encodingsBuilder.encodings(token, false);

        return new TokenWithEncodings(token, encodings);
    }

//    private QueryBuilder buildSuggestionScope(TaskContext taskContext) {
//        BoolQueryBuilder scopeQueryBuilder = new BoolQueryBuilder();
//        for (TagScope suggestionScope : taskContext.suggestionScopes) {
//            scopeQueryBuilder.should(QueryBuilders.nestedQuery("fieldStats", QueryBuilders.termQuery("fieldStats.fieldName", suggestionScope.getEntityName()), ScoreMode.None));
//        }
//
//        scopeQueryBuilder.minimumNumberShouldMatch(1);
//
//        return scopeQueryBuilder;
//    }

    private static class TokenQueryBuilder {
        private final EncodingQueryBuilder encodingQueryBuilder = new EncodingQueryBuilder();
        private final ExactTokenQueryBuilder exactTokenQueryBuilder = new ExactTokenQueryBuilder();

        private QueryBuilder query(TokenWithEncodings tokenWithEncodings) {
//            return functionScoreQuery(innerQuery(tokenWithEncodings), ScoreFunctionBuilders.fieldValueFactorFunction("encodings.weight").missing(1));
            return functionScoreQuery(innerQuery(tokenWithEncodings), ScoreFunctionBuilders.scriptFunction(new Script("_score * (doc['encodings.tokenType'] == 'EdgeGram' ? 0.5 : 1.0)")));
        }

        private QueryBuilder innerQuery(TokenWithEncodings tokenWithEncodings) {
            BoolQueryBuilder encodingsQuery = encodingQueryBuilder.query(tokenWithEncodings);

            QueryBuilder exactTokenQuery = exactTokenQueryBuilder.query(tokenWithEncodings);

            if (encodingsQuery != null) {
                return disMaxQuery().add(exactTokenQuery).add(encodingsQuery);
            } else {
                return exactTokenQuery;
            }
        }
    }

    private static class EncodingQueryBuilder {
        private final PhoneticEncodingQueryBuilder phoneticEncodingQueryBuilder = new PhoneticEncodingQueryBuilder();
        private final NGramEncodingQueryBuilder nGramEncodingQueryBuilder = new NGramEncodingQueryBuilder();

        private BoolQueryBuilder query(TokenWithEncodings tokenWithEncodings) {
            BoolQueryBuilder encodingsQuery = boolQuery()/*.disableCoord(true)*/;

            Set<String> ngramEncodings = tokenWithEncodings.encodings.get(TokenIndexConstants.Encoding.NGRAM_ENCODING);
            if (ngramEncodings != null && ngramEncodings.size() > 0) {
                int minShouldMatch = Math.min(Math.max((int) Math.floor(0.375 * ngramEncodings.size()), MINIMUM_NUMBER_SHOULD_MATCH), ngramEncodings.size());
                QueryBuilder ngramQuery = nGramEncodingQueryBuilder.query(ngramEncodings, TokenIndexConstants.Encoding.NGRAM_ENCODING, minShouldMatch);

                if (ngramQuery != null) {
                    encodingsQuery.must(ngramQuery);
                }
            }

            QueryBuilder phoneticQuery = phoneticEncodingQueryBuilder.query(tokenWithEncodings.encodings);
            if (phoneticQuery != null) {
                encodingsQuery.must(phoneticQuery);
            }

            QueryBuilder ngramQuery = nGramEncodingQueryBuilder.boundaryQuery(tokenWithEncodings.encodings);
            if (ngramQuery != null) {
                encodingsQuery.must(ngramQuery);
            }

            if (encodingsQuery.hasClauses()) {
                return encodingsQuery;
            }

            return null;
        }
    }

    private static class NGramEncodingQueryBuilder {
        private QueryBuilder query(Set<String> encodings, String field, int minimumShouldMatch) {
            if (encodings == null) {
                return null;
            }

            BoolQueryBuilder queryBuilder = boolQuery()/*.disableCoord(true)*/;

            final int totalLength = totalLength(encodings);

            encodings.forEach(w -> {
                float boost = Math.round(w.length() * 100.0f / totalLength);
                queryBuilder.should(/*constantScoreQuery(*/QueryBuilders.termQuery(encodingNestedField(field), w)).boost(boost)/*)*/;
            });

            queryBuilder.minimumNumberShouldMatch(minimumShouldMatch);

            return queryBuilder;
        }

        // build ngram query
        private QueryBuilder boundaryQuery(Map<String, Set<String>> encodings) {
            BoolQueryBuilder boolQueryBuilder = boolQuery().minimumNumberShouldMatch(1)/*.disableCoord(true)*/;

            Set<String> ngramStartEncodings = encodings.get(TokenIndexConstants.Encoding.NGRAM_START_ENCODING);
            if (ngramStartEncodings != null) {
                QueryBuilder query = query(ngramStartEncodings, TokenIndexConstants.Encoding.NGRAM_START_ENCODING, 1);
                boolQueryBuilder.should(query);
            }

            Set<String> ngramEndEncodings = encodings.get(TokenIndexConstants.Encoding.NGRAM_END_ENCODING);
            if (ngramEndEncodings != null) {
                QueryBuilder query = query(ngramEndEncodings, TokenIndexConstants.Encoding.NGRAM_END_ENCODING, 1);
                boolQueryBuilder.should(query);
            }

            if (boolQueryBuilder.hasClauses()) {
                return boolQueryBuilder;
            }

            return null;
        }

        private int totalLength(Set<String> encodings) {
            int totalLength = 0;

            for (String e : encodings) {
                totalLength += e.length();
            }

            return totalLength;
        }
    }

    private static class PhoneticEncodingQueryBuilder {

        private QueryBuilder query(Map<String, Set<String>> encodings) {
            BoolQueryBuilder phoneticQueryBuilder = boolQuery().minimumNumberShouldMatch(1)/*.disableCoord(true)*/;

            encodings
                    .entrySet()
                    .forEach(e -> {
                        if (StringUtils.startsWith(e.getKey(), TokenIndexConstants.Encoding.NGRAM_ENCODING)) {
                            return;
                        }

                        BoolQueryBuilder boolQueryBuilder = boolQuery()/*.disableCoord(true)*/;

                        float boost = Math.round(100.0f / e.getValue().size());

                        e.getValue().forEach(w -> boolQueryBuilder.should(/*constantScoreQuery(*/QueryBuilders.termQuery(encodingNestedField(e.getKey()), w)).boost(boost))/*)*/;

                        phoneticQueryBuilder.should(boolQueryBuilder);
                    });

            if (phoneticQueryBuilder.hasClauses()) {
                return phoneticQueryBuilder;
            }

            return null;
        }
    }

    private static class ExactTokenQueryBuilder {

        private QueryBuilder query(TokenWithEncodings tokenWithEncodings) {
            return /*constantScoreQuery(*/termQuery(TOKEN_NESTED_FIELD, tokenWithEncodings.token)/*)*/.boost(EXACT_TERM_BOOST);
        }
    }

}
