import java.util.concurrent.ConcurrentHashMap;

public class RedisCache {


  // Simple Redis Cache Implementation
  // The cache is stored in memory and shared among all the clients

  private static ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

  // need error handling for the case when key is not present.
  public static String get(String key) {
    return map.get(key);
  }

  public static void set(String key,String value) {
    map.put(key, value);
  }
}
