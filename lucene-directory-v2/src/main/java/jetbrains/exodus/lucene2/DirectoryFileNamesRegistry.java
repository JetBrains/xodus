package jetbrains.exodus.lucene2;

import org.apache.commons.lang3.StringUtils;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class DirectoryFileNamesRegistry {

    private final Path basePath;
    private final ConcurrentHashMap<String, FileDescription> byIndexName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, FileDescription> byAddress = new ConcurrentHashMap<>();

    public DirectoryFileNamesRegistry(Path basePath) {
        this.basePath = basePath;
    }

    public FileDescription tryRegisterFile(String realFileName) {

        if (realFileName == null) {
            return null;
        }
        final var m = ADDRESS_PLUS_FILE_NAME_PATTERN.matcher(realFileName);
        if (!m.matches()) {
            return null;
        }

        final var description = createFileDescription(m.group(2), Long.parseLong(m.group(1)));
        register(description);
        return description;
    }

    public void register(FileDescription description) {

        // todo: should we check for duplicates?
        byIndexName.put(description.indexName(), description);
        byAddress.put(description.address(), description);
    }

    public FileDescription createFileDescription(String indexName, long address) {
        if (StringUtils.isBlank(indexName)) {
            throw new IllegalArgumentException("indexName is empty or null");
        }

        if (address < 0) {
            throw new IllegalArgumentException("address is negative: " + address);
        }

        return new FileDescriptionImpl(
                indexName,
                address,
                basePath.resolve(address + "_" + indexName),
                basePath.resolve(IV_FILE_PREFIX + indexName)
        );
    }

    public boolean existsIndexName(String indexName) {
        return byIndexName.containsKey(indexName);
    }

    public FileDescription lookupByIndexName(String indexName) throws FileNotFoundException {
        final var description = byIndexName.get(indexName);

        if (description == null) {
            throw new FileNotFoundException("File " + indexName + " does not exist.");
        }

        return description;
    }

    public FileDescription lookupByAddress(long address) {
        final var description = byAddress.get(address);

        if (description == null) {
            throw new IllegalArgumentException("File with address " + address + " does not exist.");
        }

        return description;
    }

    public FileDescription removeByIndexName(String indexName) throws FileNotFoundException {
        final var fileDesc = byIndexName.remove(indexName);
        if (fileDesc == null) {
            throw new FileNotFoundException("File " + indexName + " does not exist");
        }

        byAddress.remove(fileDesc.address());

        return fileDesc;
    }

    public Set<String> indexNames() {
        return byIndexName.keySet();
    }

    public interface FileDescription {
        String indexName();

        long address();

        Path filePath();

        Path ivFilePath();
    }

    private record FileDescriptionImpl(
            String indexName,
            long address,
            Path filePath,
            Path ivFilePath
    ) implements FileDescription {
    }

    private static final Pattern ADDRESS_PLUS_FILE_NAME_PATTERN = Pattern.compile("^(\\d+)_(.+)$");
    private static final String IV_FILE_PREFIX = "iv_";

    public static String indexNameFromIvFileName(String ivFileName) {

        if (ivFileName != null && ivFileName.startsWith(IV_FILE_PREFIX)) {
            return ivFileName.substring(3);
        }

        return null;
    }
}
