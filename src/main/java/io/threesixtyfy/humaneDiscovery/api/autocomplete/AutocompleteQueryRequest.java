package io.threesixtyfy.humaneDiscovery.api.autocomplete;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryRequest;

public class AutocompleteQueryRequest extends QueryRequest<AutocompleteQuerySource, AutocompleteQueryRequest> {

    public AutocompleteQueryRequest() {
        super(new AutocompleteQuerySource());
    }

}
