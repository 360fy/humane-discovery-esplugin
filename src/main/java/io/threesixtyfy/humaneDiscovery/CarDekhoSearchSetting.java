package io.threesixtyfy.humaneDiscovery;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CarDekhoSearchSetting {

    private final SearchApiSetting autocompleteSettings = new SearchApiSetting(this);
    private final SearchApiSetting searchSettings = new SearchApiSetting(this);

    private Map<String, IntentField> intentFields = new HashMap<>();

    public CarDekhoSearchSetting() {
        addIntentField("brand", 11.0f)
                .addIntentField("model", 10.0f)
                .addIntentField("variant", 9.0f)
                .addIntentField("fuelType", 8.0f)
                .addIntentField("sellerType", 7.0f)
                .addIntentField("ownershipType", 6.0f)
                .addIntentField("city", 5.0f)
                .addIntentField("dealer_name", 4.0f)
                .addIntentField("news_title", 3.0f)
                .addIntentField("pageType", 2.0f);

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

    private CarDekhoSearchSetting addIntentField(String field, float weight) {
        IntentField intentField = new IntentField(field, weight);
        intentFields.put(field, intentField);

        return this;
    }

    // TODO: add support for transform here
    public static class SearchApiSetting {
        private final Map<String, TypeSetting> typeSettings = new HashMap<>();

        private final CarDekhoSearchSetting setting;

        public SearchApiSetting(CarDekhoSearchSetting setting) {
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
        private final Set<IntentField> intentFields = new HashSet<>();

        private final CarDekhoSearchSetting setting;

        public TypeSetting(String type, CarDekhoSearchSetting setting) {
            this.type = type;
            this.setting = setting;
        }

        private TypeSetting addQueryField(String field, float weight) {
            QueryField queryField = new QueryField(field, weight);

            queryFields.add(queryField);

            return this;
        }

        private TypeSetting addIntentField(String field) {
            intentFields.add(this.setting.intentFields.get(field));

            return this;
        }

        public String getType() {
            return type;
        }

        public Collection<QueryField> getQueryFields() {
            return queryFields;
        }

        public Collection<IntentField> getIntentFields() {
            return intentFields;
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
