import java.io.*;
import java.net.Socket;
import java.util.logging.Logger;

public class ClientHandler implements Runnable {
  private Socket clientSocket;

  public ClientHandler(Socket clientSocket) {
    this.clientSocket = clientSocket;
  }

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
          writer.write("+PONG\r\n");
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
          writer.write("+OK\r\n");
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
    String format = String.format("$%d\r\n%s\r\n", message.length(), message);
    return format;
  }
}