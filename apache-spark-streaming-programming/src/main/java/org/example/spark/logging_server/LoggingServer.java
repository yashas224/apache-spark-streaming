package org.example.spark.logging_server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/*
verify using nc localhost 9090
 */
public class LoggingServer {

  public static void main(String[] args) throws IOException, InterruptedException {
    ServerSocket echoSocket = new ServerSocket(9091);
    Socket socket = echoSocket.accept();
    System.out.println("Connection made");

    while(true) {
      PrintWriter out =
         new PrintWriter(socket.getOutputStream(), true);
      double sample = Math.random();
      String level = "DEBUG";
      if(sample < 0.0001) {
        level = "FATAL";
      } else if(sample < 0.01) {
        level = "ERROR";
      } else if(sample < 0.1) {
        level = "WARN";
      } else if(sample < 0.5) {
        level = "INFO";
      }
      out.println(level + "," + new java.util.Date());
      Thread.sleep(1);
    }
  }
}
