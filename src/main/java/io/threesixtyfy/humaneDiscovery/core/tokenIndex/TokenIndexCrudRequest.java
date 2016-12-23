package io.threesixtyfy.humaneDiscovery.core.tokenIndex;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;

public class TokenIndexCrudRequest {

    private final RequestType requestType;
    private final String indexName;

    public TokenIndexCrudRequest(RequestType requestType, String indexName) {
        this.requestType = requestType;
        this.indexName = indexName;
    }

    public TokenIndexCrudRequest(RequestType requestType) {
        this(requestType, null);
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public String getIndexName() {
        return indexName;
    }

    enum RequestType {
        CREATE,
        DELETE,
        STOP
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }
}
