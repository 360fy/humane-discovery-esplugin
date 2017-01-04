package io.threesixtyfy.humaneDiscovery.core.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class GsonUtils {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    public static <T> String toJson(T object) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> GSON.toJson(object));
    }

    public static <T> T fromJson(String content, Class<T> classType) {
        return AccessController.doPrivileged((PrivilegedAction<T>) () -> GSON.fromJson(content, classType));
    }
}
