package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.index.analysis.AnalysisService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionsBuilder.DUMMY_FIELD;
import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionsBuilder.HUMANE_QUERY_ANALYZER;

public class TokensBuilder {

    private static final TokensBuilder instance = new TokensBuilder();

    public static TokensBuilder INSTANCE() {
        return instance;
    }

    private TokensBuilder() {
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
