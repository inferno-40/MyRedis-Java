import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
public class Main {

  private static final String exceptionName = "IOException: ";
  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.out.println("Logs from your program will appear here!");
    //  Uncomment this block to pass the first stage
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 6379;
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting
      // SO_REUSEADDR ensures that we don't run into 'Address already in use'
      // errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.

      System.out.println("Server is listening on port: " + port);
      while (true) {
        clientSocket = serverSocket.accept();
        System.out.println("New Client Connected.");
        executorService.submit(new ClientHandler(clientSocket));
      }
    } catch (IOException e) {
      System.out.println(exceptionName + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
        executorService.shutdown();
      } catch (IOException e) {
        System.out.println(exceptionName + e.getMessage());
      }
    }
  }
}