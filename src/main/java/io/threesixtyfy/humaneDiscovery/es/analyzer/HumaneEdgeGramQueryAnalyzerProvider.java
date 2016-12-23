package io.threesixtyfy.humaneDiscovery.es.analyzer;

import io.threesixtyfy.humaneDiscovery.es.tokenFilter.PrefixTokenFilterFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.LowerCaseTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

public class HumaneEdgeGramQueryAnalyzerProvider extends AbstractIndexAnalyzerProvider<CustomAnalyzer> {

    public static final String NAME = "humane_edgeGram_query_analyzer";

    private final CustomAnalyzer customAnalyzer;

    public HumaneEdgeGramQueryAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        customAnalyzer = new CustomAnalyzer(new StandardTokenizerFactory(indexSettings, environment, name, settings),
                null,
                new TokenFilterFactory[]{
                        new LowerCaseTokenFilterFactory(indexSettings, environment, name, settings),
//                        new StopTokenFilterFactory(index, indexSettingsService, env, name, settings),
                        new PrefixTokenFilterFactory(indexSettings, environment, name,
                                Settings.builder().put(AnalyzerConstants.VALUE_SETTING, AnalyzerConstants.EDGE_GRAM_PREFIX)
                                        .build())
                });
    }

    @Override
    public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}
