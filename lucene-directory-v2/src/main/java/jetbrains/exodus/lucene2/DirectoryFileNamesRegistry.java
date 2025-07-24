package jetbrains.exodus.lucene2;

import jetbrains.exodus.ExodusException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class DirectoryFileNamesRegistry {

    private final Path basePath;
    private final ConcurrentHashMap<String, Long> byIndexName = new ConcurrentHashMap<>();
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

        // the order of puts is important here, because we don't want the file to be visible, while its FileDescription
        // is not yet initalized in the index
        byAddress.put(description.address(), description);
        byIndexName.put(description.indexName(), description.address());
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
                getFilePath(indexName, address),
                getIvPath(indexName)
        );
    }

    private @NotNull Path getIvPath(String indexName) {
        return basePath.resolve(IV_FILE_PREFIX + indexName);
    }

    private @NotNull Path getFilePath(String indexName, long address) {
        return basePath.resolve(address + "_" + indexName);
    }

    public void rename(FileDescription sourceDesc, String newName, boolean includingIv) {
        // this will allow new name to be instantly available
        byIndexName.put(newName, sourceDesc.address());

        // making the old name unavailable for read
        byIndexName.remove(sourceDesc.indexName());

        final var destPath = getFilePath(newName, sourceDesc.address());
        final var destIvPath = getIvPath(newName);

        // atomically moving the file and changing the file description
        byAddress.compute(sourceDesc.address(), (address, oldDesc) -> {

            try {
                // todo: IV and metadata: copy + move main file + remove old
                // todo: merge IV and metadata
                DirUtil.tryMoveAtomically(sourceDesc.filePath(), destPath);
                if (includingIv) {
                    DirUtil.tryMoveAtomically(sourceDesc.ivFilePath(), destIvPath);
                }
            } catch (IOException e) {
                throw new ExodusException(e);
            }
            return new FileDescriptionImpl(
                    newName,
                    address,
                    destPath,
                    destIvPath
            );
        });
    }

    public boolean existsIndexName(String indexName) {
        return byIndexName.containsKey(indexName);
    }

    public FileDescription lookupByIndexName(String indexName) throws FileNotFoundException {
        final var address = byIndexName.get(indexName);
        final var description = address == null ? null : byAddress.get(address);

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
        final var address = byIndexName.remove(indexName);
        final var description = address == null ? null : byAddress.remove(address);
        if (description == null) {
            throw new FileNotFoundException("File " + indexName + " does not exist");
        }

        return description;
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
