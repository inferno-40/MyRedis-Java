import config.RDBConfig;
import redisdatastructure.RedisCache;

import java.io.*;
import java.net.Socket;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.logging.Logger;

import resp.RespConvertor;


public class ClientHandler implements Runnable {

  public final static String OK_BULK_STRING = "+OK\r\n";
  public final static String PONG_BULK_STRING = "+PONG\r";
  public final static String FORMAT_BULK_STRING = "$%d\r\n%s\r\n";
  public final static String NULL_BULK_STRING = "$-1\r\n";

  private Socket clientSocket;

  public ClientHandler(Socket clientSocket) {
    this.clientSocket = clientSocket;
  }


  @Override
  public void run() {
    try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(clientSocket.getInputStream()));
         PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {
      String content;
      // can move this whole block to a new class.
      while (true) {
        content = reader.readLine();
        if (content == null) {
          Logger.getLogger("Client disconnected.");
          break;
        }
        int length = Integer.parseInt(content.substring(1, 2));
        String[] command = parseArray(length, content, reader);
        executeCommand(command, writer);

      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private static String[] parseArray(int length, String data,
                                     BufferedReader br) {
    String[] parts = new String[length];
    for (int i = 0; i < length; i++) {
      try {
        String line = br.readLine();
        if (line.startsWith("$")) {
          parts[i] = parseBulkString(line, br);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return parts;
  }

  private static String parseBulkString(String line, BufferedReader br)
          throws IOException {
    int strLength =
            Integer.parseInt(line.substring(1)); // Get the string length
    char[] buffer = new char[strLength];
    int charsRead = br.read(
            buffer, 0, strLength); // Read exactly strLength characters to buffer
    if (charsRead < strLength) {
      throw new IOException("Expected " + strLength + " characters but read " +
              charsRead);
    }
    br.readLine(); // Read the trailing \r\n
    return new String(buffer);
  }

  private void executeCommand(String[] command, PrintWriter writer) throws IOException {
    String response;
    switch (command[0].toLowerCase()) {
      case "ping":
        sendPong(writer);
        break;
      case "echo":
        if (command.length != 2) {
          throw new IOException(
                  "-ERR wrong number of arguments for 'echo' command.\r");
        }
        response = handleEcho(command);
        sendResponse(writer, response);
        break;
      case "set":
        if (command.length < 3 && command.length % 2 == 0) {
          throw new IOException(
                  "-ERR wrong number of arguments for 'set' command.\r");
        }
        response = handleSet(command);
        sendResponse(writer, response);
        break;
      case "get":
        if (command.length != 2) {
          throw new IOException(
                  "-ERR wrong number of arguments for 'get' command.\r");
        }
        response = handleGet(command);
        sendResponse(writer, response);
        break;
      case "config":
        if (command.length < 3) {
          throw new IOException(
                  "-ERR wrong number of arguments for 'config' command.\r");
        }
        response = handleConfig(command);
        sendResponse(writer, response);
        break;
      case "keys":
        response = handleKeys(command);
        sendResponse(writer, response);
      default:
        break;
    }
  }

  private String handleKeys(String[] command) {
    try {
      String pattern = command[1];
      String bulkArrayResponse = "";
      if (pattern.equalsIgnoreCase("*")) {
        bulkArrayResponse = RespConvertor.toRESPArray(RedisCache.getAllKeys());
      }
      return bulkArrayResponse;
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return null;
  }

  private String handleConfig(String[] command) {
    try {
      if (RDBConfig.isRDBEnabled) {
        String configParam = command[2];
        List<String> input;
        if (configParam.equalsIgnoreCase("dir")) {
          input = List.of(configParam, RDBConfig.getInstance().getDir());
        } else {
          input = List.of(configParam, RDBConfig.getInstance().getDbFileName());
        }
        return RespConvertor.toRESPArray(input);
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return null;
  }

  private String handleGet(String[] command) {
    try {
      String key = command[1];
      System.out.println("Key: " + key);
      RedisCache.Value value = RedisCache.getValue(key);
      System.out.println("value: " + value);
      long now = (Timestamp.valueOf(LocalDateTime.now()).getTime());
      if (value == null || (value.canExpire() && now >= value.expiry())) {
        RedisCache.delete(key);
        return RespConvertor.toBulkString(null);
      }
      return RespConvertor.toBulkString(value.value());
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return null;
  }

  private String handleEcho(String[] command) {
    if (command.length > 1) {
      String text = command[1];
      return RespConvertor.toBulkString(text);
    }
    return "";
  }

  private String handleSet(String[] command) throws IllegalArgumentException {
    try {
      if (command.length > 2) {
        String key = command[1];
        String val = command[2];
        long expiry = 0;
        boolean canExpire = false;
        if (command.length > 4) {
          canExpire = true;
          expiry = Long.parseLong(command[4]);
        }

        RedisCache.Value value = new RedisCache.Value(val, canExpire, (Timestamp.valueOf(LocalDateTime.now()).getTime()) + expiry);
        RedisCache.setValue(key, value);
        return OK_BULK_STRING;
      } else {
        throw new IllegalArgumentException();
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return null;
  }

  private void sendPong(PrintWriter writer) {
    writer.println(PONG_BULK_STRING);
  }

  private void sendResponse(PrintWriter writer, String response) {
    writer.print(response);
    writer.flush();
  }

}