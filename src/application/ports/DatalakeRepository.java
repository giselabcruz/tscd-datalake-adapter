package src.application.ports;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

public interface DatalakeRepository {

    void saveBook(int bookId, Path stagingDir, LocalDateTime timestamp) throws IOException;

    boolean exists(int bookId) throws IOException;

    String relativePathFor(int bookId, LocalDateTime timestamp);

    List<Integer> listBooks() throws IOException;
}
