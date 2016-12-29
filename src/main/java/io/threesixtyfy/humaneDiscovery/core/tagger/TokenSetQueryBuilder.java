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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.Fields.ENCODINGS;
import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.TOKEN_NESTED_FIELD;
import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.encodingNestedField;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.disMaxQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class TokenSetQueryBuilder {

    private static final Logger logger = Loggers.getLogger(TokenSetQueryBuilder.class);

    private static final float EXACT_TERM_BOOST = 2050.0f;
    private static final int MINIMUM_NUMBER_SHOULD_MATCH = 2;
//    private static final String SCORE_SCRIPT = "_score / doc['tokenCount'].value";

    private final TokenQueryBuilder tokenQueryBuilder = new TokenQueryBuilder();
    private final EncodingsBuilder encodingsBuilder = new EncodingsBuilder();

    public QueryBuilder buildQuery(Collection<TagWeight> tagWeights, List<String> tokens) {
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

        //        QueryBuilder queryBuilder = functionScoreQuery(nestedQuery,
//                ScoreFunctionBuilders.scriptFunction(new Script(SCORE_SCRIPT,
//                        ScriptService.ScriptType.INLINE,
//                        "expression",
//                        null)));

        return simplifyBoolQuery(boolQueryBuilder);
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
//        for (TagWeight suggestionScope : taskContext.suggestionScopes) {
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
            return functionScoreQuery(innerQuery(tokenWithEncodings), ScoreFunctionBuilders.fieldValueFactorFunction("encodings.weight").missing(1));
//            return functionScoreQuery(innerQuery(tokenWithEncodings), ScoreFunctionBuilders.scriptFunction(new Script("_score * (doc['encodings.tokenType'].value == 'EdgeGram' ? 0.5 : 1.0)")));
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
        private final NGramStartEncodingQueryBuilder nGramStartEncodingQueryBuilder = new NGramStartEncodingQueryBuilder();
        private final NGramEndEncodingQueryBuilder nGramEndEncodingQueryBuilder = new NGramEndEncodingQueryBuilder();

        private BoolQueryBuilder query(TokenWithEncodings tokenWithEncodings) {
            BoolQueryBuilder encodingsQuery = boolQuery()/*.disableCoord(true)*/.minimumNumberShouldMatch(1);

            Consumer<QueryBuilder> consumer = (queryBuilder) -> {
                if (queryBuilder != null) {
                    encodingsQuery.should(queryBuilder);
                }
            };

            nGramStartEncodingQueryBuilder.query(tokenWithEncodings.encodings, consumer);
            nGramEncodingQueryBuilder.query(tokenWithEncodings.encodings, consumer);
            phoneticEncodingQueryBuilder.query(tokenWithEncodings.encodings, consumer);
            nGramEndEncodingQueryBuilder.query(tokenWithEncodings.encodings, consumer);

            if (encodingsQuery.hasClauses()) {
                return encodingsQuery;
            }

            return null;
        }
    }

    private static class BaseNGramQueryBuilder {
        protected QueryBuilder query(Map<String, Set<String>> encodingsMap, String field, Function<Set<String>, Integer> minimumShouldMatch, float boost) {
            Set<String> encodings = encodingsMap.get(field);

            if (encodings == null || encodings.size() == 0) {
                return null;
            }

            BoolQueryBuilder queryBuilder = boolQuery()/*.boost(boost)*//*.disableCoord(true)*/;

            final int totalLength = totalLength(encodings);

            encodings.forEach(w -> {
                float encodingBoost = Math.round(w.length() * boost / totalLength);
                queryBuilder.should(constantScoreQuery(QueryBuilders.termQuery(encodingNestedField(field), w)).boost(encodingBoost));
            });

            queryBuilder.minimumNumberShouldMatch(minimumShouldMatch.apply(encodings));

            return queryBuilder;
        }

        protected int totalLength(Set<String> encodings) {
            int totalLength = 0;

            for (String e : encodings) {
                totalLength += e.length();
            }

            return totalLength;
        }

        protected int minShouldMatch(Set<String> encodings) {
            return 1;
        }
    }

    private static class NGramEndEncodingQueryBuilder extends BaseNGramQueryBuilder {
        private void query(Map<String, Set<String>> encodings, Consumer<QueryBuilder> consumer) {
            consumer.accept(query(encodings, TokenIndexConstants.Encoding.NGRAM_END_ENCODING, this::minShouldMatch, 200.0f));
        }
    }

    private static class NGramStartEncodingQueryBuilder extends BaseNGramQueryBuilder {
        private void query(Map<String, Set<String>> encodings, Consumer<QueryBuilder> consumer) {
            consumer.accept(query(encodings, TokenIndexConstants.Encoding.NGRAM_START_ENCODING, this::minShouldMatch, 200.0f));
        }
    }

    private static class NGramEncodingQueryBuilder extends BaseNGramQueryBuilder {
        protected int minShouldMatch(Set<String> encodings) {
            return Math.min(Math.max((int) Math.floor(0.20 * encodings.size()), MINIMUM_NUMBER_SHOULD_MATCH), encodings.size());
        }

        private void query(Map<String, Set<String>> encodings, Consumer<QueryBuilder> consumer) {
            consumer.accept(query(encodings, TokenIndexConstants.Encoding.NGRAM_ENCODING, this::minShouldMatch, 100.0f));
        }
    }


    private static class PhoneticEncodingQueryBuilder {

        private void query(Map<String, Set<String>> encodings, Consumer<QueryBuilder> consumer) {
            encodings
                    .entrySet()
                    .forEach(e -> {
                        if (StringUtils.startsWith(e.getKey(), TokenIndexConstants.Encoding.NGRAM_ENCODING)) {
                            return;
                        }

                        float boost = Math.round(200.0f / e.getValue().size());

                        BoolQueryBuilder boolQueryBuilder = boolQuery()/*.boost(boost)*//*.disableCoord(true)*/;

                        e.getValue().forEach(w -> boolQueryBuilder.should(constantScoreQuery(QueryBuilders.termQuery(encodingNestedField(e.getKey()), w)).boost(boost)));
                        consumer.accept(boolQueryBuilder);
                    });
        }
    }

    private static class ExactTokenQueryBuilder {

        private QueryBuilder query(TokenWithEncodings tokenWithEncodings) {
            return /*constantScoreQuery(*/termQuery(TOKEN_NESTED_FIELD, tokenWithEncodings.token)/*)*/.boost(EXACT_TERM_BOOST);
        }
    }

}
