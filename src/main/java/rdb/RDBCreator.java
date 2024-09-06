package rdb;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import redisdatastructure.RedisCache;
import resp.RespConvertor;

public class RDBCreator {
  static final String magicString = "REDIS";
  static final String version = "0007";
  private DataOutputStream dos;
  ConcurrentHashMap<String, RedisCache.Value> redisMap;
  public RDBCreator(DataOutputStream dos,
                    ConcurrentHashMap<String, RedisCache.Value> redisMap) {
    this.dos = dos;
    this.redisMap = redisMap;
  }
  public void writeRDB() throws IOException {
    dos.write(magicString.getBytes(StandardCharsets.UTF_8));
    dos.write(version.getBytes(StandardCharsets.UTF_8));
    dos.writeByte(0xFE);
    dos.writeByte(0x00);
    for (Map.Entry<String, RedisCache.Value> entry : redisMap.entrySet()) {
      if (entry.getValue().canExpire()) {
        dos.writeByte(0xFC);
        dos.write(String.valueOf(entry.getValue().expiry())
                .getBytes(StandardCharsets.UTF_8));
      }
      dos.writeByte(0x00);
      dos.write(RespConvertor.toBulkString(entry.getKey())
              .getBytes(StandardCharsets.UTF_8));
      dos.write(RespConvertor.toBulkString(entry.getValue().value())
              .getBytes(StandardCharsets.UTF_8));
    }
    dos.write(0xFF);
    dos.flush();
  }
}