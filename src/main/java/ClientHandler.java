import javax.swing.*;
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
      while (true) {
        content = reader.readLine();
        if (content == null) {
          Logger.getLogger("Client disconnected.");
          break;
        }
        Logger.getLogger("::" + content);
        if ("ping".equalsIgnoreCase(content)) {
          writer.write("+PONG\r\n");
          writer.flush();
        } else if ("echo".equalsIgnoreCase(content)) {
          String message = reader.readLine();
          String response = getEchoMessage(message);
          writer.write(response);
          writer.flush();
        } else if ("eof".equalsIgnoreCase(content)) {
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

  private String getEchoMessage(String message) {
    String format = String.format("$%d\r\n%s\r\n", message.length(), message);
    return format;
  }
}