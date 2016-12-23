package io.threesixtyfy.humaneDiscovery.core.tagger;

import org.elasticsearch.search.SearchHit;

import java.util.List;

public class SearchHitUtils {

    @SuppressWarnings("unchecked")
    public static <V> V fieldValue(SearchHit searchHit, String field, V defaultValue) {
        Object value = searchHit.getSource().get(field);
        if (value != null) {
            return (V) value;
        }

        return defaultValue;
    }

    @SuppressWarnings("unchecked")
    public static <V> V fieldValue(SearchHit searchHit, String field) {
        return fieldValue(searchHit, field, null);
    }

//    @SuppressWarnings("unchecked")
//    public static List<Object> fieldValues(SearchHit searchHit, String field) {
//        return (List<Object>) searchHit.getSource().get(field);
//    }

}
