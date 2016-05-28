package io.threesixtyfy.humaneDiscovery.analyzer;

import io.threesixtyfy.humaneDiscovery.tokenFilter.EdgeGramTokenFilterFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.LowerCaseTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.settings.IndexSettingsService;

public class HumaneDescriptiveTextAnalyzerProvider extends AbstractIndexAnalyzerProvider<CustomAnalyzer> {

    public static final int DEFAULT_MIN_EDGE_GRAM_SIZE = 2;

    private final CustomAnalyzer customAnalyzer;

    @Inject
    public HumaneDescriptiveTextAnalyzerProvider(Index index, IndexSettingsService indexSettingsService, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettingsService.indexSettings(), name, settings);

        int minGram = settings.getAsInt("min_gram", DEFAULT_MIN_EDGE_GRAM_SIZE);

        customAnalyzer = new CustomAnalyzer(new StandardTokenizerFactory(index, indexSettingsService, name, settings),
                null,
                new TokenFilterFactory[]{
                        new LowerCaseTokenFilterFactory(index, indexSettingsService, name, settings),
                        new EdgeGramTokenFilterFactory(index, indexSettingsService, name, Settings.builder()
                                .put("min_gram", minGram)
                                .build())
                });
    }

    @Override
    public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}
