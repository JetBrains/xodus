package jetbrains.vectoriadb.client;

public record IndexMetadata(int maximumConnectionsPerVertex, int  maximumCandidatesReturned, int  compressionRatio, float distanceMultiplier) {
}
