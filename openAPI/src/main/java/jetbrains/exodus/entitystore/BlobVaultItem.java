package jetbrains.exodus.entitystore;

import org.jetbrains.annotations.NotNull;

public interface BlobVaultItem {

    long getHandle();

    @NotNull
    String getLocation();

    boolean exists();

}
