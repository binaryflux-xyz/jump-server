package xyz.binaryflux.jumpserver;

import io.netty.buffer.ByteBuf;
import java.io.OutputStream;
import java.nio.file.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RetryFileUtil {
    private static final String RETRY_DIR = "/tmp/jumpserver-retry/";
    private static final long MAX_FILE_SIZE = 150L * 1024 * 1024; // 150MB
    private static final long MAX_TOTAL_SIZE = 5L * 1024 * 1024 * 1024; // 5GB

    public static void writeFailedBatch(List<ByteBuf> records, boolean compressed) {
        try {
            Files.createDirectories(Paths.get(RETRY_DIR));
            long totalSize = Files.list(Paths.get(RETRY_DIR))
                .mapToLong(p -> p.toFile().length())
                .sum();
            if (totalSize >= MAX_TOTAL_SIZE) {
                System.err.println("[RetryFileUtil] Retry dir exceeds 5GB, dropping new failed batch");
                return;
            }
            long currentFileSize = 0;
            OutputStream out = null;
            Path filePath = null;
            int fileIndex = 0;
            for (int i = 0; i < records.size(); i++) {
                ByteBuf buf = records.get(i);
                int recordSize = buf.readableBytes();
                // If adding this record would exceed 150MB, close current file and start a new one
                if (out == null || currentFileSize + recordSize > MAX_FILE_SIZE) {
                    if (out != null) {
                        out.close();
                        System.err.println("[RetryFileUtil] Wrote failed batch to: " + filePath);
                    }
                    // Check total size again before creating a new file
                    totalSize = Files.list(Paths.get(RETRY_DIR))
                        .mapToLong(p -> p.toFile().length())
                        .sum();
                    if (totalSize >= MAX_TOTAL_SIZE) {
                        System.err.println("[RetryFileUtil] Retry dir exceeds 5GB, dropping remaining failed records");
                        break;
                    }
                    String filename = RETRY_DIR + System.currentTimeMillis() + "-" + UUID.randomUUID() + "-" + fileIndex++ + (compressed ? ".gz" : ".bin");
                    filePath = Paths.get(filename);
                    out = Files.newOutputStream(filePath, StandardOpenOption.CREATE_NEW);
                    currentFileSize = 0;
                }
                byte[] data = new byte[recordSize];
                buf.getBytes(buf.readerIndex(), data);
                out.write(data);
                currentFileSize += recordSize;
            }
            if (out != null) {
                out.close();
                System.err.println("[RetryFileUtil] Wrote failed batch to: " + filePath);
            }
        } catch (Exception e) {
            System.err.println("[RetryFileUtil] Failed to write failed batch: " + e.getMessage());
        }
    }

    // Daemon to retry sending failed batches every 5 minutes
    public static void startRetryDaemon(java.util.function.Consumer<byte[]> sender) {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                Files.createDirectories(Paths.get(RETRY_DIR));
                Files.list(Paths.get(RETRY_DIR)).forEach(path -> {
                    try {
                        byte[] data = Files.readAllBytes(path);
                        sender.accept(data);
                        Files.delete(path);
                        System.err.println("[RetryFileUtil] Retried and deleted: " + path);
                    } catch (Exception e) {
                        System.err.println("[RetryFileUtil] Retry failed for " + path + ": " + e.getMessage());
                    }
                });
            } catch (Exception e) {
                System.err.println("[RetryFileUtil] Retry daemon error: " + e.getMessage());
            }
        }, 0, 5, TimeUnit.MINUTES);
    }
} 