package io.threesixtyfy.humaneDiscovery.api.autocomplete;

import org.elasticsearch.common.ParseField;

public class AutocompleteConstants {

    public static final int DEFAULT_COUNT = 10;
    public static final int DEFAULT_PAGE = 0;

    public static final ParseField COUNT_FIELD = new ParseField("count").withDeprecation("c");
    public static final ParseField PAGE_FIELD = new ParseField("page").withDeprecation("p");
    public static final ParseField TYPE_FIELD = new ParseField("type").withDeprecation("t");
    public static final ParseField SECTION_FIELD = new ParseField("section").withDeprecation("s");
//    public static final ParseField SORT_FIELD = new ParseField("sort").withDeprecation("o");
//    public static final ParseField FILTER_FIELD = new ParseField("filter").withDeprecation("f");
    public static final ParseField FORMAT_FIELD = new ParseField("format");

}
