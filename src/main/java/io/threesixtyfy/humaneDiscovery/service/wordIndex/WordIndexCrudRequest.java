package io.threesixtyfy.humaneDiscovery.service.wordIndex;

public class WordIndexCrudRequest {

    private final RequestType requestType;
    private final String indexName;

    public WordIndexCrudRequest(RequestType requestType, String indexName) {
        this.requestType = requestType;
        this.indexName = indexName;
    }

    public WordIndexCrudRequest(RequestType requestType) {
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
}
