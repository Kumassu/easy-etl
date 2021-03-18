package song.pan.etl.common.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import song.pan.etl.rdbms.ConnectionProperties;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public class CacheHolder {


    public static final Cache<Object, Object> CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build();


    static {
        ConnectionProperties properties = new ConnectionProperties();
        properties.setUrl("jdbc:mysql://localhost:3306/?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai");
        properties.setUser("root");
        properties.setPassword("123456");
        set(CacheType.SERVER, "MYSQL", properties);
    }

    public static Object get(CacheType type, Object key) {
        return CACHE.getIfPresent(type + ":" + key);
    }

    public static void set(CacheType type, Object key, Object value) {
        CACHE.put(type + ":" + key, value);
    }

    public static void remove(CacheType type, Object key) {
        CACHE.invalidate(type + ":" + key);
    }


    public static List<Object> get(CacheType type) {
        return CACHE.asMap().entrySet().stream().filter(k -> k.toString().startsWith(type + ":"))
                .map(Map.Entry::getValue).collect(Collectors.toList());
    }


    public enum CacheType {
        SERVER,
        ETL_TASK,
        ;
    }


}
