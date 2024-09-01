import java.io.*;
import java.net.Socket;
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
          System.out.println("Client disconnected.");
          break;
        }
        System.out.println("::" + content);
        if ("ping".equalsIgnoreCase(content)) {
          writer.write("+PONG\r\n");
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
}