import java.io.*;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class ClientHandler implements Runnable {

  public final static String OK_BULK_STRING = "+OK\r";
  public final static String PONG_BULK_STRING = "+PONG\r";
  public final static String FORMAT_BULK_STRING = "$%d\r\n%s\r";
  public final static String NULL_BULK_STRING = "$-1\r";

  private Socket clientSocket;

  public ClientHandler(Socket clientSocket) {
    this.clientSocket = clientSocket;
  }

  ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

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
    switch (command[0].toLowerCase()) {
      case "ping":
        sendPong(writer);
        break;
      case "echo":
        if (command.length != 2) {
          throw new IOException(
                  "-ERR wrong number of arguments for 'echo' command.\r");
        }
        sendBulkString(writer, command[1]);
        break;
      case "set":
        if (command.length < 3 && command.length % 2 == 0) {
          throw new IOException(
                  "-ERR wrong number of arguments for 'set' command.\r");
        }
        String key = command[1];
        String value = command[2];
        System.out.println(key);
        System.out.println(value);
        RedisCache.set(key, value);

        if (command.length == 5) {
          if (command[3].equalsIgnoreCase("px")) {
            long delay = Long.parseLong(command[4]);
            System.out.println(delay);
            scheduler.schedule(
                    () -> RedisCache.delete(key), delay, TimeUnit.MILLISECONDS
            );
          }
        }
        writer.println(OK_BULK_STRING);
        break;
      case "get":
        if (command.length != 2) {
          throw new IOException(
                  "-ERR wrong number of arguments for 'get' command.\r");
        }
        String response = RedisCache.get(command[1]);
        if (response == NULL_BULK_STRING) {
          writer.println(NULL_BULK_STRING);
        } else {
          sendBulkString(writer, response);
        }
        break;
      case "config":
        System.out.println(command.length);
        if(command.length < 3) {
          throw new IOException(
                  "-ERR wrong number of arguments for 'config' command.\r");
        }
        String configName = command[2];
        String configValue = command[3];
        sendBulkString(writer, configName);
        sendBulkString(writer, configValue);
      default:
        break;
    }
  }

  private void sendPong(PrintWriter writer) {
    writer.println(PONG_BULK_STRING);
  }

  private void sendBulkString(PrintWriter writer, String response) {
    writer.println(String.format(FORMAT_BULK_STRING, response.length(), response));
  }

}