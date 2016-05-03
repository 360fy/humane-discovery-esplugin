package io.threesixtyfy.humaneDiscovery.tokenFilter;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.settings.IndexSettingsService;

public class EdgeGramTokenFilterFactory extends AbstractTokenFilterFactory {

    public static final int DEFAULT_MIN_EDGE_GRAM_SIZE = 2;
    public static final int DEFAULT_MAX_EDGE_GRAM_SIZE = 20;

    private final int minGram;

    private final int maxGram;

    @Inject
    public EdgeGramTokenFilterFactory(Index index, IndexSettingsService indexSettingsService, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettingsService.getSettings(), name, settings);

        this.minGram = settings.getAsInt("min_gram", DEFAULT_MIN_EDGE_GRAM_SIZE);
        this.maxGram = settings.getAsInt("max_gram", DEFAULT_MAX_EDGE_GRAM_SIZE);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new EdgeGramTokenFilter(tokenStream, minGram, maxGram);
    }
}
