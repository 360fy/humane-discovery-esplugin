package io.threesixtyfy.humaneDiscovery.es.analyzer;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.LowerCaseTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

public class HumaneQueryAnalyzerProvider extends AbstractIndexAnalyzerProvider<CustomAnalyzer> {

    public static final String NAME = "humane_query_analyzer";

    private final CustomAnalyzer customAnalyzer;

    public HumaneQueryAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        customAnalyzer = new CustomAnalyzer(new StandardTokenizerFactory(indexSettings, environment, name, settings),
                null,
                new TokenFilterFactory[]{
                        new LowerCaseTokenFilterFactory(indexSettings, environment, name, settings)
//                        new StopTokenFilterFactory(index, indexSettingsService, env, name, settings)
                });
    }

    @Override
    public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}
