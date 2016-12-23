package io.threesixtyfy.humaneDiscovery.api.commons;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchParseException;

public abstract class QueryRestAction<QS extends QuerySource<QS>, QR extends QueryRequest<QS, QR>> extends BaseRestAction<QR> {

    protected static final ParseField QUERY_FIELD = new ParseField("query").withDeprecation("text", "q");

    @Inject
    public QueryRestAction(Settings settings, RestController controller) {
        super(settings, controller);
    }

    protected void parseParams(QR baseRequest, RestRequest request, ParseFieldMatcher parseFieldMatcher, BytesReference source) throws SearchParseException {
        for (String key : request.params().keySet()) {
            if (parseFieldMatcher.match(key, QUERY_FIELD)) {
                String queryParam = request.param(key);

                if (queryParam != null) {
                    baseRequest.querySource().query(queryParam);
                }
            }
        }
    }

    protected void parseSource(QR baseRequest, ParseFieldMatcher parseFieldMatcher, BytesReference source) throws SearchParseException {
        // nothing to parseObject...
        if (source == null || source.length() == 0) {
            return;
        }
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);

            // parseObject query source
            baseRequest.querySource().fromXContent(parser, parseFieldMatcher);

        } catch (Throwable e) {
            throw new ElasticsearchParseException("failed to parseObject.", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

}
