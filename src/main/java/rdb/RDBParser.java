package rdb;

import config.RDBConfig;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import redisdatastructure.RedisCache;

public class RDBParser {
  static DataInputStream inStream;

  public static void parseRDB() {
    try {
      inStream = new DataInputStream(
              new FileInputStream(RDBConfig.INSTANCE.getFullPath()));
      String header = parseHeader();
      System.out.println("Header: " + header);
      Map<String, String> metaData = parseMetaData();
      System.out.println("MetaData: " + metaData);
      parseDB();
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

  private static String parseHeader() throws IOException {
    String header = new String(inStream.readNBytes(9));
    return header;
  }

  private static Map<String, String> parseMetaData() throws IOException {
    Map<String, String> metaData = new HashMap<>();
    while (true) {
      int currByte = inStream.readUnsignedByte();
      System.out.println("currByte: " + currByte + ", " + (currByte == 0xFA));
      if (currByte == 0xFA) {
        int checkDBStartByte = inStream.readUnsignedByte();
        System.out.println("checkDBStartByte: " + checkDBStartByte);
        int keySize = parseSize(checkDBStartByte);
        String key = new String(inStream.readNBytes(keySize));
        System.out.println("key: " + key);
        int valSize = parseSize(inStream.readUnsignedByte());
        String value = "";
        if (valSize == 0) {
          value = String.valueOf(inStream.readUnsignedByte());
        } else {
          value = new String(inStream.readNBytes(valSize));
        }
        System.out.println("value: " + value);
        metaData.put(key, value);
      } else {
        break;
      }
    }
    return metaData;
  }

  private static void parseDB() throws IOException {
    int dbIndex = parseSize(inStream.readUnsignedByte());
    int type = inStream.readUnsignedByte();
    boolean resizePresent = false;
    if (type == 0xFB) {
      int hashTableSize = parseSize(inStream.readUnsignedByte());
      int expiresHashTableSize = parseSize(inStream.readUnsignedByte());
      resizePresent = true;
    }
    while (true) {
      if (!resizePresent) {
        type = inStream.readUnsignedByte();
      } else {
        resizePresent = false;
      }
      if (type == 0xFF) {
        break;
      }
      boolean canExpire = false;
      long expiry = 0;
      if (type == 0xFC || type == 0xFD) {
        canExpire = true;
        if (type == 0xFC) {
          expiry = readLitleEndianLong();
        } else {
          expiry = readLittleEndianInt();
        }
        inStream.readUnsignedByte();
        type = inStream.readUnsignedByte();
      }
      int keySize = parseSize(type);
      if (keySize == 0) {
        continue;
      }
      String key = new String(inStream.readNBytes(keySize));
      System.out.println(key);
      int valSize = parseSize(inStream.readUnsignedByte());
      String value = new String(inStream.readNBytes(valSize));
      if (!key.isEmpty()) {
        RedisCache.setValue(key, new RedisCache.Value(value, canExpire, expiry));
      }
    }
  }

  private static long readLittleEndianInt() throws IOException {
    return ((long) inStream.readUnsignedByte() |
            ((long) inStream.readUnsignedByte() << 8) |
            ((long) inStream.readUnsignedByte() << 16) |
            ((long) inStream.readUnsignedByte() << 24));
  }

  private static long readLitleEndianLong() throws IOException {
    return ((long) inStream.readUnsignedByte() |
            ((long) inStream.readUnsignedByte() << 8) |
            ((long) inStream.readUnsignedByte() << 16) |
            ((long) inStream.readUnsignedByte() << 24) |
            ((long) inStream.readUnsignedByte() << 32) |
            ((long) inStream.readUnsignedByte() << 40) |
            ((long) inStream.readUnsignedByte() << 48) |
            ((long) inStream.readUnsignedByte() << 56));
  }

  private static int parseSize(int sizeEncodingByte) throws IOException {
    int encoding = sizeEncodingByte & 0xC0;
    int size = 0;
    System.out.println("sizeEncodingByte: " + sizeEncodingByte);
    System.out.println("encoding: " + encoding);
    switch (encoding) {
      case 0x00:
        size = (sizeEncodingByte) & 0x3F;
        break;
      case 0x40:
        int nextByte = inStream.readUnsignedByte();
        System.out.println("nextByte: " + nextByte);
        int prevByte = ((sizeEncodingByte) & 0x3F);
        System.out.println("prevByte: " + prevByte);
        size = ((prevByte & 0xFF) << 8) | (nextByte & 0xFF);
        break;
      case 0x80:
        size = inStream.readInt();
        break;
      case 0xC0:
        break;
      default:
        throw new IOException("Bad Read");
    }
    System.out.println("size: " + size);
    return size;
  }
}