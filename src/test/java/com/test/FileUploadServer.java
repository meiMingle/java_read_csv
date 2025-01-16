package com.test;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUploadServer {

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
                // 读取请求头中的 Content-Disposition
                String contentDisposition = exchange.getRequestHeaders().getFirst("Content-Disposition");
                String fileName = extractFileName(contentDisposition);

                // 获取用户主目录
                String homeDir = System.getProperty("user.home");
                // 创建文件输出流，使用上传的文件名
                File file = new File(homeDir, fileName);
                
                // 读取请求体
                try (InputStream inputStream = exchange.getRequestBody();
                     FileOutputStream fileOutputStream = new FileOutputStream(file)) {
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
                try (OutputStream outputStream = exchange.getResponseBody()) {
                    outputStream.write(response.getBytes());
                }
            } else {
                // 只支持 POST 请求
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
            }
        }

        private String extractFileName(String contentDisposition) {
            if (contentDisposition != null && contentDisposition.contains("filename=")) {
                String[] parts = contentDisposition.split(";");
                for (String part : parts) {
                    part = part.trim();
                    if (part.startsWith("filename=")) {
                        // 去掉引号并返回文件名
                        return part.substring(10, part.length() - 1);
                    }
                }
            }
            return "uploaded_file"; // 默认文件名
        }
    }
}
