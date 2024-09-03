import java.io.*;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class ClientHandler implements Runnable {

  public final static String OK_BULK_STRING = "+OK\r";
  public final static String PONG_BULK_STRING = "+PONG\r";
  public final static String ECHO_BULK_STRING = "$%d\r\n%s\r";
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
         PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(),true)) {
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
          writer.println(response);
        }
        else if ("set".equalsIgnoreCase(content)){
          reader.readLine();
          String key = reader.readLine();
          reader.readLine();
          String value = reader.readLine();
          RedisCache.set(key, value);
          reader.readLine();
          String exp = reader.readLine();
          if(exp != null && "px".equalsIgnoreCase(exp)){
            System.out.println(value);
            reader.readLine();
            String time = reader.readLine();
            Runnable task = () -> {
              RedisCache.delete(key);
            };
            scheduler.schedule(task, Long.parseLong(time), TimeUnit.MILLISECONDS);
            scheduler.shutdown();
          }
          writer.println(OK_BULK_STRING);
        }
        else if("get".equalsIgnoreCase(content)) {
          reader.readLine();
          String key = reader.readLine();
          String value = RedisCache.get(key);
          String response = (value == NULL_BULK_STRING) ? NULL_BULK_STRING : getRESPMessage(value);
          writer.println(response);
        }else if ("eof".equalsIgnoreCase(content)) {
          Logger.getLogger("EOF received. Closing Connection.");
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