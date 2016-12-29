package io.threesixtyfy.humaneDiscovery.core.instance;

import io.threesixtyfy.humaneDiscovery.core.tagger.DefaultTagWeight;
import io.threesixtyfy.humaneDiscovery.core.tagger.TagWeight;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CarDekhoInstanceContext implements InstanceContext {

    public static final String NAME = "carDekho";

    private final SearchApiSetting autocompleteSettings = new SearchApiSetting(this);
    private final SearchApiSetting searchSettings = new SearchApiSetting(this);

    private Map<String, TagWeight> tagWeightsMap = new HashMap<>();

    public CarDekhoInstanceContext() {
        addTagWeight("brand", 10.5f)
                .addTagWeight("model", 10.25f)
                .addTagWeight("variant", 10.0f)
                .addTagWeight("pageType", 9.0f)
                .addTagWeight("fuelType", 8.0f)
                .addTagWeight("sellerType", 7.0f)
                .addTagWeight("ownershipType", 6.0f)
                .addTagWeight("dealer_name", 5.0f)
                .addTagWeight("news_title", 4.0f)
                .addTagWeight("city", 3.0f);

        autocompleteSettings.addType("new_car_variant")
                .addQueryField("variant", 10.0f)
                .addIntentField("variant");
        autocompleteSettings.addType("new_car_model")
                .addQueryField("model", 10.0f)
                .addIntentField("model");
        autocompleteSettings.addType("new_car_brand")
                .addQueryField("brand", 10.0f)
                .addIntentField("brand");

        searchSettings.addType("new_car_variant")
                .addQueryField("variant", 10.0f)
                .addIntentField("variant");
        searchSettings.addType("new_car_model")
                .addQueryField("model", 10.0f)
                .addIntentField("model");
        searchSettings.addType("new_car_brand")
                .addQueryField("brand", 10.0f)
                .addIntentField("brand");
        searchSettings.addType("used_car")
                .addQueryField("variant", 10.0f)
                .addQueryField("model", 10.0f)
                .addQueryField("brand", 10.0f)
                .addQueryField("ownershipType", 10.0f)
                .addQueryField("sellerType", 10.0f)
                .addIntentField("variant")
                .addIntentField("model")
                .addIntentField("brand")
                .addIntentField("ownershipType")
                .addIntentField("sellerType");
        searchSettings.addType("car_news")
                .addQueryField("variant", 10.0f)
                .addQueryField("model", 10.0f)
                .addQueryField("brand", 10.0f)
                .addQueryField("news_title", 10.0f)
                .addIntentField("variant")
                .addIntentField("model")
                .addIntentField("brand");
        searchSettings.addType("new_car_dealer")
                .addQueryField("brand", 10.0f)
                .addQueryField("city", 10.0f)
                .addQueryField("name", 10.0f)
                .addIntentField("dealer_name")
                .addIntentField("city")
                .addIntentField("brand");
    }

    public SearchApiSetting getAutocompleteSettings() {
        return autocompleteSettings;
    }

    public SearchApiSetting getSearchSettings() {
        return searchSettings;
    }

    private CarDekhoInstanceContext addTagWeight(String field, float weight) {
        DefaultTagWeight intentField = new DefaultTagWeight(field, weight);
        tagWeightsMap.put(field, intentField);

        return this;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Collection<TagWeight> getTagWeights() {
        return tagWeightsMap.values();
    }

    @Override
    public Map<String, TagWeight> getTagWeightsMap() {
        return tagWeightsMap;
    }

    // TODO: add support for transform here
    public static class SearchApiSetting {
        private final Map<String, TypeSetting> typeSettings = new HashMap<>();

        private final CarDekhoInstanceContext setting;

        public SearchApiSetting(CarDekhoInstanceContext setting) {
            this.setting = setting;
        }

        private TypeSetting addType(String type) {
            TypeSetting typeSetting = new TypeSetting(type, setting);

            typeSettings.put(type, typeSetting);

            return typeSetting;
        }

        public Map<String, TypeSetting> getTypeSettings() {
            return typeSettings;
        }
    }

    public static class TypeSetting {
        private String type;
        private final Set<QueryField> queryFields = new HashSet<>();
        private final Set<TagWeight> tagWeights = new HashSet<>();

        private final CarDekhoInstanceContext setting;

        public TypeSetting(String type, CarDekhoInstanceContext setting) {
            this.type = type;
            this.setting = setting;
        }

        private TypeSetting addQueryField(String field, float weight) {
            QueryField queryField = new QueryField(field, weight);

            queryFields.add(queryField);

            return this;
        }

        private TypeSetting addIntentField(String field) {
            tagWeights.add(this.setting.tagWeightsMap.get(field));

            return this;
        }

        public String getType() {
            return type;
        }

        public Collection<QueryField> getQueryFields() {
            return queryFields;
        }

        public Collection<TagWeight> getTagWeights() {
            return tagWeights;
        }
    }

    public static class QueryField {
        private String field;
        private String path;
        private float weight;

        public QueryField(String field, float weight) {
            this(field, null, weight);
        }

        public QueryField(String field, String path, float weight) {
            this.field = field;
            this.path = path;
            this.weight = weight;
        }

        public String getField() {
            return field;
        }

        public String getPath() {
            return path;
        }

        public float getWeight() {
            return weight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            QueryField that = (QueryField) o;

            return field.equals(that.field);
        }

        @Override
        public int hashCode() {
            return field.hashCode();
        }
    }

    public static class IntentField {
        private String field;
        private float weight;

        public IntentField(String field, float weight) {
            this.field = field;
            this.weight = weight;
        }

        public String getField() {
            return field;
        }

        public float getWeight() {
            return weight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            IntentField that = (IntentField) o;

            return field.equals(that.field);
        }

        @Override
        public int hashCode() {
            return field.hashCode();
        }
    }

}
