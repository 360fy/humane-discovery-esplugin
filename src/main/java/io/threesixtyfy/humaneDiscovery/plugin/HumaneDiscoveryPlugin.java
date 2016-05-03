package io.threesixtyfy.humaneDiscovery.plugin;

import io.threesixtyfy.humaneDiscovery.query.HumaneQueryParser;
import io.threesixtyfy.humaneDiscovery.query.MultiFieldHumaneQueryParser;
import io.threesixtyfy.humaneDiscovery.tokenFilter.EdgeGramTokenFilterFactory;
import io.threesixtyfy.humaneDiscovery.tokenFilter.HumaneTokenFilterFactory;
import io.threesixtyfy.humaneDiscovery.tokenFilter.PrefixTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;

public class HumaneDiscoveryPlugin extends Plugin {

    @Override
    public String name() {
        return "humane_discovery";
    }

    @Override
    public String description() {
        return "humane discovery";
    }

    public void onModule(IndicesModule module) {
        module.registerQueryParser(HumaneQueryParser.class);
        module.registerQueryParser(MultiFieldHumaneQueryParser.class);
    }

    public void onModule(AnalysisModule module) {
        module.addTokenFilter("humane", HumaneTokenFilterFactory.class);
        module.addTokenFilter("prefix", PrefixTokenFilterFactory.class);
        module.addTokenFilter("humane_edgeGram", EdgeGramTokenFilterFactory.class);
    }

}
