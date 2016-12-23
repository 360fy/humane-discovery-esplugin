package io.threesixtyfy.humaneDiscovery.core.conjuncts;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.index.analysis.AnalysisService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TokensBuilder {

    private static final TokensBuilder INSTANCE = new TokensBuilder();
    private static final String HUMANE_QUERY_ANALYZER = "humane_query_analyzer";
    private static final String DUMMY_FIELD = "dummy_field";

    private TokensBuilder() {
    }

    public static TokensBuilder INSTANCE() {
        return INSTANCE;
    }

    public List<String> tokens(AnalysisService analysisService, String query) throws IOException {
        Analyzer analyzer = analysisService.analyzer(HUMANE_QUERY_ANALYZER);
        if (analyzer == null) {
            throw new RuntimeException("No humane_query_analyzer found");
        }

        TokenStream tokenStream = analyzer.tokenStream(DUMMY_FIELD, query);

        tokenStream.reset();

        CharTermAttribute termAttribute = tokenStream.getAttribute(CharTermAttribute.class);

        List<String> words = new ArrayList<>();
        while (tokenStream.incrementToken()) {
            words.add(termAttribute.toString());
        }

        tokenStream.close();

        return words;
    }
}
