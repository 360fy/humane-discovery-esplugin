package io.threesixtyfy.humaneDiscovery.es.analyzer;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.EdgeNGramTokenFilterFactory;
import org.elasticsearch.index.analysis.LowerCaseTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

public class HumaneVernacularAnalyzerProvider extends AbstractIndexAnalyzerProvider<CustomAnalyzer> {

    public static final String NAME = "humane_vernacular_analyzer";
    private static final int MIN_GRAMS_DEFAULT = 3;
    private static final int MAX_GRAMS_DEFAULT = 20;

    private final CustomAnalyzer customAnalyzer;

    public HumaneVernacularAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        customAnalyzer = new CustomAnalyzer(new StandardTokenizerFactory(indexSettings, environment, name, settings),
                null,
                new TokenFilterFactory[]{
                        new LowerCaseTokenFilterFactory(indexSettings, environment, name, settings),
                        new EdgeNGramTokenFilterFactory(indexSettings, environment, name, Settings.builder().put(AnalyzerConstants.MIN_GRAM_SETTING, MIN_GRAMS_DEFAULT).put(AnalyzerConstants.MAX_GRAM_SETTING, MAX_GRAMS_DEFAULT).build())
                });
    }

    @Override
    public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}
