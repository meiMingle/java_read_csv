package com.test;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.net.InetSocketAddress;

public class FileUploadServer0 {

    public static void main(String[] args) throws Exception {
        // 创建一个 HTTP 服务器，监听在 8080 端口
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/upload", new FileUploadHandler());
        server.setExecutor(null); // 创建默认的执行器
        server.start();
        System.out.println("Server started on port 8080");
    }

    static class FileUploadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                // 读取请求体
                InputStream inputStream = exchange.getRequestBody();
                // 获取用户主目录
                String homeDir = System.getProperty("user.home");
                // 创建文件输出流，文件名为 "uploaded_file"
                File file = new File(homeDir, "uploaded_file");
                try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    // 将输入流中的数据写入文件
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        fileOutputStream.write(buffer, 0, bytesRead);
                    }
                }
                // 发送响应
                String response = "File uploaded successfully to " + file.getAbsolutePath();
                exchange.sendResponseHeaders(200, response.length());
                OutputStream outputStream = exchange.getResponseBody();
                outputStream.write(response.getBytes());
                outputStream.close();
            } else {
                // 只支持 POST 请求
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
            }
        }
    }
}
