package jetbrains.exodus.diskann;

interface DistanceFunction {
    double computeDistance(float[] firstVector, float[] secondVector);

    double computeDistance(
            float[] firstVector, int firstVectorFrom, float[] secondVector,
            int secondVectorFrom, int size);
}
