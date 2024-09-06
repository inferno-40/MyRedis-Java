package redisdatastructure;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;

import config.RDBConfig;
import rdb.RDBCreator;


public class RedisCache {

  // Simple Redis Cache Implementation
  // The cache is stored in memory and shared among all the clients

  public record Value(String value, boolean canExpire, long expiry) {
    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Value{");
      sb.append("value='").append(value).append('\'');
      sb.append(", canExpire=").append(canExpire);
      sb.append(", expiry=").append(expiry);
      sb.append("}\n");
      return sb.toString();
    }
  }

  private static ConcurrentHashMap<String, Value> redisMap = new ConcurrentHashMap<>();

  // need error handling for the case when key is not present.
  public static Value getValue(String key) {
    return RedisCache.redisMap.get(key);
  }

  public static void setValue(String key, Value value) {
    RedisCache.redisMap.put(key, value);
  }

  public static void delete(String key) {
    if(RedisCache.redisMap.containsKey(key)){
      RedisCache.redisMap.remove(key);
    }
  }


  public static List<String> getAllKeys() {
    System.out.println(new ArrayList<>(RedisCache.redisMap.keySet()).size());
    return new ArrayList<>(RedisCache.redisMap.keySet());
  }

  public static void saveToRDB() {
    try {
      String rdbFilePath = RDBConfig.getInstance().getFullPath();
      File rdbFile = new File(rdbFilePath);
      if (!rdbFile.exists()) {
        rdbFile.createNewFile();
      }
      FileOutputStream fos = new FileOutputStream(rdbFilePath);
      DataOutputStream dos = new DataOutputStream(fos);
      RDBCreator rdbCreator = new RDBCreator(dos, RedisCache.redisMap);
      rdbCreator.writeRDB();
      dos.close();
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

}
