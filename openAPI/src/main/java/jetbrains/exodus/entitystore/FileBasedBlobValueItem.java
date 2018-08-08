package jetbrains.exodus.entitystore;

import org.jetbrains.annotations.NotNull;

import java.io.File;

public class FileBasedBlobValueItem implements BlobVaultItem {

    @NotNull
    private final File file;
    private final long handle;

    public FileBasedBlobValueItem(@NotNull File file, long handle) {
        this.handle = handle;
        this.file = file;
    }

    @Override
    public long getHandle() {
        return handle;
    }

    @Override
    public @NotNull String getLocation() {
        return file.getAbsolutePath();
    }

    @Override
    public boolean exists() {
        return file.exists();
    }

    @Override
    public String toString() {
        return getLocation();
    }
}
