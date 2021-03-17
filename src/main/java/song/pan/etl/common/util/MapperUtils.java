package song.pan.etl.common.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import song.pan.etl.common.exception.SystemException;

import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public class MapperUtils {


    public static Gson getGson() {
        return new GsonBuilder().setPrettyPrinting().create();
    }


    public static String prettyJson(String json) {
        return getGson().toJson(new JsonParser().parse(json).getAsJsonObject());
    }

    public static String prettyJson(Object obj) {
        return prettyJson(objToJson(obj));
    }


    public static ObjectMapper getObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH));
        return objectMapper;
    }


    public static <T> T jsonToObject(String json, Class<T> clazz) {
        try {
            return getObjectMapper().readValue(json, clazz);
        } catch (Exception e) {
            throw new SystemException(e);
        }
    }

    public static String objToJson(Object obj) {
        try {
            return getObjectMapper().writeValueAsString(obj);
        } catch (Exception e) {
            throw new SystemException(e);
        }
    }



}
