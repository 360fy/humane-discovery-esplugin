package io.threesixtyfy.humaneDiscovery.es.analyzer;

import io.threesixtyfy.humaneDiscovery.es.tokenFilter.EdgeGramTokenFilterFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.LowerCaseTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

public class HumaneTextAnalyzerProvider extends AbstractIndexAnalyzerProvider<CustomAnalyzer> {

    public static final String NAME = "humane_text_analyzer";

    private static final int MIN_EDGE_GRAM_SIZE_DEFAULT = 1;

    private final CustomAnalyzer customAnalyzer;

    public HumaneTextAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);

        int minGram = settings.getAsInt(AnalyzerConstants.MIN_GRAM_SETTING, MIN_EDGE_GRAM_SIZE_DEFAULT);

        customAnalyzer = new CustomAnalyzer(new StandardTokenizerFactory(indexSettings, environment, name, settings),
                null,
                new TokenFilterFactory[]{
                        new LowerCaseTokenFilterFactory(indexSettings, environment, name, settings),
                        new EdgeGramTokenFilterFactory(indexSettings, environment, name, Settings.builder()
                                .put(AnalyzerConstants.MIN_GRAM_SETTING, minGram)
                                .build())
                });
    }

    @Override
    public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}
