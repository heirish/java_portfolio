import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class DirectorySynchronizer extends SimpleFileVisitor<Path> {
    final Path sourcePath;
    final Path destPath;

    DirectorySynchronizer(Path sourcePath, Path destPath) {
        this.destPath = destPath;
        this.sourcePath = sourcePath;
    }

    // Invoke the pattern matching
    // method on each file.
    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
        Path newFile = destPath.resolve(sourcePath.relativize(file));
        CopyOption[] copyOptions = new CopyOption[]{COPY_ATTRIBUTES, REPLACE_EXISTING};
        try {
            if (!Files.exists(newFile) ||
                    Files.size(file) != Files.size(newFile)) {
                Files.copy(file, newFile, copyOptions);
            }
        } catch (IOException ioe) {
            //ioe.printStackTrace();
        }

        return CONTINUE;
    }


    // Invoke the pattern matching
    // method on each directory.
    @Override
    public FileVisitResult preVisitDirectory(Path dir,
                                             BasicFileAttributes attrs) {
        CopyOption[] copyOptions = new CopyOption[]{COPY_ATTRIBUTES, REPLACE_EXISTING};
        Path newDir= destPath.resolve(sourcePath.relativize(dir));
        try {
            Files.copy(dir, newDir, copyOptions);
        } catch (IOException ioe) {

        }
        return CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) {
        System.err.println(exc);
        return CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        return CONTINUE;
    }
} // class FileFinder ends
