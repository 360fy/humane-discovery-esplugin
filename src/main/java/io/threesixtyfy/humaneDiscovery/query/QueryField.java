package io.threesixtyfy.humaneDiscovery.query;

class QueryField {
    String name;
    String path;
    float boost = 1.0f;
    boolean noFuzzy = false;
    boolean vernacularOnly = false;

    @Override
    public String toString() {
        return "QueryField{" +
                "name='" + name + '\'' +
                ", path='" + path + '\'' +
                ", boost=" + boost +
                ", noFuzzy=" + noFuzzy +
                ", vernacularOnly=" + vernacularOnly +
                '}';
    }
}
