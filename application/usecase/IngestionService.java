package application.usecase;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import application.ports.DatalakeRepository;

public class IngestionService {
    private static final String START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK";
    private static final String END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK";

    private final DatalakeRepository datalakeRepo;
    private final Path stagingDir;
    private final int totalBooks;
    private final int maxRetries;
    private final Random rng = new Random();

    public IngestionService(DatalakeRepository datalakeRepo,
                            Path stagingDir,
                            int totalBooks,
                            int maxRetries) {
        this.datalakeRepo = datalakeRepo;
        this.stagingDir = stagingDir.toAbsolutePath().normalize();
        this.totalBooks = totalBooks;
        this.maxRetries = maxRetries;
    }

    public boolean downloadBookToStaging(int bookId) {
        try {
            Files.createDirectories(stagingDir);
            String url = String.format("https://www.gutenberg.org/cache/epub/%d/pg%d.txt", bookId, bookId);
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).build();
            HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            if (res.statusCode() != 200) {
                System.err.println("[ERROR] HTTP " + res.statusCode() + " when downloading book " + bookId);
                return false;
            }

            String txt = res.body();
            if (!txt.contains(START_MARKER) || !txt.contains(END_MARKER)) {
                System.err.println("[WARN] Missing markers for book " + bookId);
                return false;
            }

            String[] parts = txt.split(Pattern.quote(START_MARKER), 2);
            String header = parts[0].trim();
            String[] bodyParts = parts[1].split(Pattern.quote(END_MARKER), 2);
            String body = bodyParts[0].trim();

            Files.writeString(stagingDir.resolve(bookId + "_header.txt"), header, StandardCharsets.UTF_8);
            Files.writeString(stagingDir.resolve(bookId + "_body.txt"), body, StandardCharsets.UTF_8);

            System.out.println("[INFO] Book " + bookId + " downloaded to staging at " + stagingDir);
            return true;

        } catch (IOException | InterruptedException e) {
            System.err.println("[ERROR] Download failed for book " + bookId + ": " + e.getMessage());
            return false;
        }
    }

    public boolean moveToDatalake(int bookId, LocalDateTime ts) {
        try {
            datalakeRepo.saveBook(bookId, stagingDir, ts);
            return true;
        } catch (IOException e) {
            System.err.println("[ERROR] Failed to move book " + bookId + " to datalake: " + e.getMessage());
            return false;
        }
    }

    public boolean ingestOne(int bookId, LocalDateTime ts) {
        if (!downloadBookToStaging(bookId)) return false;
        return moveToDatalake(bookId, ts);
    }

    public boolean ingestNextRandom(Set<Integer> alreadyDownloaded, LocalDateTime ts) {
        for (int i = 0; i < maxRetries; i++) {
            int candidate = rng.nextInt(totalBooks) + 1;
            if (alreadyDownloaded.contains(candidate)) continue;
            if (ingestOne(candidate, ts)) return true;
        }
        return false;
    }

    public boolean existsInDatalake(int bookId) {
        try {
            return datalakeRepo.exists(bookId);
        } catch (IOException e) {
            System.err.println("[ERROR] Could not check existence for book " + bookId + ": " + e.getMessage());
            return false;
        }
    }

    public String relativePathFor(int bookId, LocalDateTime ts) {
        return datalakeRepo.relativePathFor(bookId, ts);
    }

    public List<Integer> listBooks() {
        try {
            return datalakeRepo.listBooks();
        } catch (IOException e) {
            System.err.println("[ERROR] Could not list books: " + e.getMessage());
            return List.of();
        }
    }
}
