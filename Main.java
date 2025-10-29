import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.List;
import application.ports.DatalakeRepository;
import application.usecase.IngestionService;
import infrastructure.S3DatalakeRepository;

public class Main {

    private static final String STAGING_PATH = env("STAGING_PATH", "staging/downloads");
    private static final String AWS_REGION   = env("AWS_REGION", "eu-west-1");
    private static final String S3_BUCKET    = env("S3_BUCKET", "mi-bucket-datalake");
    private static final String S3_PREFIX    = env("S3_PREFIX", "datalake");
    private static final int TOTAL_BOOKS     = intEnv("TOTAL_BOOKS", 70000);
    private static final int MAX_RETRIES     = intEnv("MAX_RETRIES", 10);
    private static final int PORT            = intEnv("PORT", 7070);

    private static IngestionService ingestionService;

    public static void main(String[] args) {
        System.out.println("[MAIN] Booting ingestion service + HTTP API...");
        try {
            Files.createDirectories(Paths.get(STAGING_PATH));
        } catch (Exception e) {
            System.err.println("[ERROR] Could not create staging dir: " + e.getMessage());
            return;
        }

        DatalakeRepository datalakeRepo = new S3DatalakeRepository(AWS_REGION, S3_BUCKET, S3_PREFIX);
        ingestionService = new IngestionService(datalakeRepo, Paths.get(STAGING_PATH), TOTAL_BOOKS, MAX_RETRIES);

        Javalin app = Javalin.create(cfg -> {
            cfg.http.defaultContentType = "application/json";
            cfg.router.contextPath = "/";
        });

        app.exception(Exception.class, (e, ctx) -> {
            e.printStackTrace();
            jsonError(ctx, HttpStatus.INTERNAL_SERVER_ERROR, "internal_error", e.getMessage());
        });

        app.start(PORT);

        app.get("/health", ctx -> ctx.json(Map.of("status", "ok", "backend", "S3")));
        app.post("/ingest/{book_id}", Main::downloadBook);
        app.get("/ingest/status/{book_id}", Main::checkStatus);
        app.get("/ingest/list", Main::listBooks);

        System.out.println("[API] Ingestion API running on http://localhost:" + PORT + "/");
    }

    private static void downloadBook(Context ctx) {
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
}
