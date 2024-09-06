import config.RDBConfig;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class Main {

  private static final String exceptionName = "IOException: ";

  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    Logger.getLogger("Logs from your program will appear here!");
    RDBConfig.initializeInstance(args);
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

      Logger.getLogger("Server is listening on port: " + port);
      while (true) {
        clientSocket = serverSocket.accept();
        System.out.println("New Client Connected.");
        executorService.submit(new ClientHandler(clientSocket));
      }
    } catch (IOException e) {
      Logger.getLogger(exceptionName + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
        executorService.shutdown();
      } catch (IOException e) {
        Logger.getLogger(exceptionName + e.getMessage());
      }
    }
  }
}