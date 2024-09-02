import java.io.*;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.Date;

public class ClientHandler implements Runnable {

  public final static String OK_BULK_STRING = "+OK\r\n";
  public final static String PONG_BULK_STRING = "+PONG\r\n";
  public final static String ECHO_BULK_STRING = "$%d\r\n%s\r\n";
  public final static String NULL_BULK_STRING = "$-1\r\n";

  private Socket clientSocket;

  public ClientHandler(Socket clientSocket) {
    this.clientSocket = clientSocket;
  }

  ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  @Override
  public void run() {
    try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(clientSocket.getInputStream()));
         BufferedWriter writer = new BufferedWriter(
                 new OutputStreamWriter(clientSocket.getOutputStream()))) {
      String content;
      // can move this whole block to a new class.
      while (true) {
        content = reader.readLine();
        if (content == null) {
          Logger.getLogger("Client disconnected.");
          break;
        }
        System.out.println("::" + content);
        if ("ping".equalsIgnoreCase(content)) {
          writer.write(PONG_BULK_STRING);
          writer.flush();
        } else if ("echo".equalsIgnoreCase(content)) {
          reader.readLine();
          String message = reader.readLine();
          String response = getRESPMessage(message);
          writer.write(response);
          writer.flush();
        }
        else if ("set".equalsIgnoreCase(content)){
          reader.readLine();
          String key = reader.readLine();
          reader.readLine();
          String value = reader.readLine();
          RedisCache.set(key, value);
          reader.readLine();
          if("px".equalsIgnoreCase(reader.readLine())){
            reader.readLine();
            String time = reader.readLine();
            Runnable task = () -> {
              RedisCache.delete(key);
            };
            scheduler.schedule(task,Integer.parseInt(time)*1000L, TimeUnit.SECONDS);
          }
          writer.write(OK_BULK_STRING);
          writer.flush();
        }
        else if("get".equalsIgnoreCase(content)) {
          reader.readLine();
          String key = reader.readLine();
          String response = getRESPMessage(RedisCache.get(key));
          writer.write(response);
          writer.flush();
        }else if ("eof".equalsIgnoreCase(content)) {
          System.out.println("EOF received. Closing Connection.");
          break;
        }
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

  private String getRESPMessage(String message) {
    String format = String.format(ECHO_BULK_STRING, message.length(), message);
    return format;
  }
}