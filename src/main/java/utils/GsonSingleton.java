package utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Сингтон, необходимый для JSON парсинга
 */
public class GsonSingleton {
    public static Gson Gson = new GsonBuilder().create();
}
