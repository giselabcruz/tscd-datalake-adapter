package com.ingestion;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.List;
import com.ingestion.application.usecase.IngestionService;
import com.ingestion.infrastructure.S3DatalakeStorage;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class Main {

    private static final String STAGING_PATH = env("STAGING_PATH", "staging/downloads");
    private static final String AWS_REGION   = env("AWS_REGION", US_EAST_1.toString());
    private static final String S3_BUCKET    = env("S3_BUCKET", "my-bucket-datalake");
    private static final String S3_PREFIX    = env("S3_PREFIX", "datalake");
    private static final int PORT            = intEnv("PORT", 7070);
    private static final String endpoint = env("S3_ENDPOINT_URL", "http://localhost:4566");
    private static final String AWS_ACCESS_KEY_ID = env("AWS_ACCESS_KEY_ID","test");
    private static final String AWS_SECRET_ACCESS_KEY = env("AWS_SECRET_ACCESS_KEY","test");


    private static IngestionService ingestionService;

    public static void main(String[] args) {
        var app = ConfigServer();
        app.start(PORT);
        System.out.println("[API] Ingestion API running on http://localhost:" + PORT + "/");
    }

    private static Javalin ConfigServer() {
        System.out.println("[MAIN] Booting ingestion service + HTTP API...");
        Javalin app = Javalin.create(cfg -> {
            cfg.http.defaultContentType = "application/json";
            cfg.router.contextPath = "/";
        });
        app.exception(Exception.class, Main::handleException);
        try (var s3 = s3Client()) {
            var s3DatalakeStorage = new S3DatalakeStorage(s3, S3_BUCKET, S3_PREFIX);
            ingestionService = new IngestionService(s3DatalakeStorage, Paths.get(STAGING_PATH));
            app.get("/health", ctx -> ctx.json(Map.of(
                    "status", "ok",
                    "backend", "S3",
                    "region", AWS_REGION,
                    "bucket", S3_BUCKET
            )));
            app.post("/ingest/{book_id}", Main::downloadBook);
            app.get("/ingest/status/{book_id}", Main::checkStatus);
            app.get("/ingest/list", Main::listBooks);
            return app;
        } catch (Exception e) {
            throw new RuntimeException("Could not initialize ingestion service", e);
        }
    }
    private static S3Client s3Client() {
        return S3Client.builder()
                .region(Region.of(AWS_REGION))
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(credentialsProvider())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
    }

    @NotNull
    private static AwsCredentialsProvider credentialsProvider() {
        return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                        AWS_ACCESS_KEY_ID,
                        AWS_SECRET_ACCESS_KEY
                )
        );
    }

    private static void downloadBook(Context ctx) throws IOException {
        Files.createDirectories(Paths.get(STAGING_PATH));
        Integer bookId = parseBookId(ctx);
        if (bookId == null) return;

        System.out.println("[API] Received ingestion request for book " + bookId);
        boolean staged = ingestionService.downloadBookToStaging(bookId);
        if (!staged) {
            jsonError(ctx, HttpStatus.BAD_REQUEST, "download_failed", "Download failed or invalid book");
            return;
        }

        LocalDateTime ts = LocalDateTime.now();
        boolean moved = ingestionService.moveToDatalake(bookId, ts);
        if (!moved) {
            jsonError(ctx, HttpStatus.INTERNAL_SERVER_ERROR, "datalake_move_failed", "Failed to move files to datalake");
            return;
        }

        String date = ts.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String hour = ts.format(DateTimeFormatter.ofPattern("HH"));
        String path = ingestionService.relativePathFor(bookId, ts);

        ctx.status(HttpStatus.OK).json(Map.of(
                "book_id", bookId,
                "status", "downloaded",
                "path", path,
                "date", date,
                "hour", hour,
                "backend", "S3"
        ));
    }

    private static void checkStatus(Context ctx) {
        Integer bookId = parseBookId(ctx);
        if (bookId == null) return;

        boolean exists = ingestionService.existsInDatalake(bookId);
        ctx.json(Map.of(
                "book_id", bookId,
                "status", exists ? "available" : "not_found",
                "backend", "S3"
        ));
    }

    private static void listBooks(Context ctx) {
        List<Integer> books = ingestionService.listBooks();
        ctx.json(Map.of(
                "count", books.size(),
                "books", books,
                "backend", "S3"
        ));
    }

    private static Integer parseBookId(Context ctx) {
        String raw = ctx.pathParam("book_id");
        try {
            int id = Integer.parseInt(raw);
            if (id <= 0) {
                jsonError(ctx, HttpStatus.BAD_REQUEST, "invalid_book_id", "book_id must be a positive integer");
                return null;
            }
            return id;
        } catch (NumberFormatException nfe) {
            jsonError(ctx, HttpStatus.BAD_REQUEST, "invalid_book_id", "book_id must be an integer");
            return null;
        }
    }

    private static void jsonError(Context ctx, HttpStatus status, String code, String message) {
        ctx.status(status).json(Map.of(
                "error", Map.of(
                        "code", code,
                        "message", message
                )
        ));
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    private static int intEnv(String key, int def) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return def;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException ignored) {
            return def;
        }
    }

    private static void handleException(@org.jetbrains.annotations.NotNull Exception e, @org.jetbrains.annotations.NotNull Context ctx) {
        e.printStackTrace();
        jsonError(ctx, HttpStatus.INTERNAL_SERVER_ERROR, "internal_error", e.getMessage());
    }

}
