package jetbrains.vectoriadb.client;

public enum IndexState {
    CREATING,
    CREATED,
    UPLOADING,
    UPLOADED,
    IN_BUILD_QUEUE,
    BUILDING,
    BUILT,
    BROKEN
}
