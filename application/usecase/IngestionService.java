package application.usecase;

import application.ports.DatalakeRepository;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

public class IngestionService {

    private static final String[] START_MARKERS = {
            "*** START OF THE PROJECT GUTENBERG EBOOK",
            "*** START OF THIS PROJECT GUTENBERG EBOOK"
    };
    private static final String[] END_MARKERS = {
            "*** END OF THE PROJECT GUTENBERG EBOOK",
            "*** END OF THIS PROJECT GUTENBERG EBOOK"
    };

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private final DatalakeRepository datalakeRepo;
    private final Path stagingDir;
    private final int totalBooks;
    private final int maxRetries;
    private final Random rng = new Random();

    public IngestionService(DatalakeRepository datalakeRepo, Path stagingDir, int totalBooks, int maxRetries) {
        this.datalakeRepo = datalakeRepo;
        this.stagingDir = stagingDir.toAbsolutePath().normalize();
        this.totalBooks = totalBooks;
        this.maxRetries = maxRetries;
    }

    public boolean downloadBookToStaging(int bookId) {
        try {
            Files.createDirectories(stagingDir);
            String url = "https://www.gutenberg.org/cache/epub/" + bookId + "/pg" + bookId + ".txt";
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .header("User-Agent", "TAHS-Ingestion/1.0")
                    .build();
            HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (res.statusCode() != 200) return false;

            String txt = res.body();
            int s = indexOfAny(txt, START_MARKERS, String::length);
            int e = lastIndexOfAny(txt, END_MARKERS);
            if (s < 0 || e < 0 || e <= s) return false;

            String header = txt.substring(0, s).trim();
            String body = txt.substring(s, e).replaceFirst("^\\Q" + leadingMarker(txt, s, START_MARKERS) + "\\E", "").trim();

            Path headerTmp = stagingDir.resolve(bookId + "_header.txt.tmp");
            Path bodyTmp = stagingDir.resolve(bookId + "_body.txt.tmp");
            Path headerDst = stagingDir.resolve(bookId + "_header.txt");
            Path bodyDst = stagingDir.resolve(bookId + "_body.txt");

            Files.writeString(headerTmp, header, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.writeString(bodyTmp, body, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            Files.move(headerTmp, headerDst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            Files.move(bodyTmp, bodyDst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            return true;
        } catch (IOException | InterruptedException ex) {
            return false;
        }
    }

    public boolean moveToDatalake(int bookId, LocalDateTime ts) {
        try {
            datalakeRepo.saveBook(bookId, stagingDir, ts);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean ingestOne(int bookId, LocalDateTime ts) {
        if (!downloadBookToStaging(bookId)) return false;
        return moveToDatalake(bookId, ts);
    }

    public boolean ingestNextRandom(Set<Integer> alreadyDownloaded, LocalDateTime ts) {
        for (int i = 0; i < maxRetries; i++) {
            int candidate = rng.nextInt(Math.max(1, totalBooks)) + 1;
            if (alreadyDownloaded.contains(candidate)) continue;
            if (ingestOne(candidate, ts)) return true;
        }
        return false;
    }

    public boolean existsInDatalake(int bookId) {
        try {
            return datalakeRepo.exists(bookId);
        } catch (IOException e) {
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
            return List.of();
        }
    }

    private static int indexOfAny(String s, String[] needles, Function<String, Integer> advance) {
        int best = -1;
        for (String n : needles) {
            int i = s.indexOf(n);
            if (i >= 0 && (best < 0 || i < best)) best = i + advance.apply(n);
        }
        return best;
    }

    private static int lastIndexOfAny(String s, String[] needles) {
        int best = -1;
        for (String n : needles) {
            int i = s.lastIndexOf(n);
            if (i >= 0 && i > best) best = i;
        }
        return best;
    }

    private static String leadingMarker(String s, int startIncluded, String[] markers) {
        for (String m : markers) {
            int i = s.indexOf(m);
            if (i >= 0 && i + m.length() == startIncluded) return m;
        }
        return markers[0];
    }
}
