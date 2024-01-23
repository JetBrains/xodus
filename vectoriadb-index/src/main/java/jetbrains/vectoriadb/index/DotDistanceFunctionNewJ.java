package jetbrains.vectoriadb.index;

import java.lang.foreign.MemorySegment;

public class DotDistanceFunctionNewJ implements DistanceFunction {
    @Override
    public float computeDistance(float[] firstVector, int firstVectorFrom, float[] secondVector, int secondVectorFrom, int size) {
        return -VectorOperationsJ.innerProduct(firstVector, firstVectorFrom, secondVector, secondVectorFrom, size);
    }

    @Override
    public float computeDistance(MemorySegment firstSegment, long firstSegmentFromOffset, float[] secondVector, int secondVectorOffset, int size) {
        return -VectorOperationsJ.innerProduct(firstSegment, firstSegmentFromOffset / Float.BYTES, secondVector, secondVectorOffset, size);
    }

    @Override
    public float computeDistance(MemorySegment firstSegment, long firstSegmentOffset, MemorySegment secondSegment, long secondSegmentOffset, int size) {
        return -VectorOperationsJ.innerProduct(firstSegment, firstSegmentOffset / Float.BYTES, secondSegment, secondSegmentOffset / Float.BYTES, size);
    }

    @Override
    public void computeDistance(MemorySegment originSegment, long originSegmentOffset, MemorySegment firstSegment, long firstSegmentOffset, MemorySegment secondSegment, long secondSegmentOffset, MemorySegment thirdSegment, long thirdSegmentOffset, MemorySegment fourthSegment, long fourthSegmentOffset, int size, float[] result) {
        VectorOperationsJ.innerProductBatch(
                originSegment, originSegmentOffset / Float.BYTES,
                firstSegment, firstSegmentOffset / Float.BYTES,
                secondSegment, secondSegmentOffset / Float.BYTES,
                thirdSegment, thirdSegmentOffset / Float.BYTES,
                fourthSegment, fourthSegmentOffset / Float.BYTES,
                size, result
        );
        result[0] = -result[0];
        result[1] = -result[1];
        result[2] = -result[2];
        result[3] = -result[3];
    }

    @Override
    public void computeDistance(float[] originVector, int originVectorOffset, MemorySegment firstSegment, long firstSegmentOffset, MemorySegment secondSegment, long secondSegmentOffset, MemorySegment thirdSegment, long thirdSegmentOffset, MemorySegment fourthSegment, long fourthSegmentOffset, int size, float[] result) {
        VectorOperationsJ.innerProductBatch(
                originVector, originVectorOffset,
                firstSegment, firstSegmentOffset / Float.BYTES,
                secondSegment, secondSegmentOffset / Float.BYTES,
                thirdSegment, thirdSegmentOffset / Float.BYTES,
                fourthSegment, fourthSegmentOffset / Float.BYTES,
                size, result
        );
        result[0] = -result[0];
        result[1] = -result[1];
        result[2] = -result[2];
        result[3] = -result[3];
    }

    @Override
    public void computeDistance(float[] originVector, int originVectorOffset, float[] firstVector, int firstVectorOffset, float[] secondVector, int secondVectorOffset, float[] thirdVector, int thirdVectorOffset, float[] fourthVector, int fourthVectorOffset, float[] result, int size) {
        VectorOperationsJ.innerProductBatch(
                originVector, originVectorOffset,
                firstVector, firstVectorOffset,
                secondVector, secondVectorOffset,
                thirdVector, thirdVectorOffset,
                fourthVector, fourthVectorOffset,
                size, result
        );
        result[0] = -result[0];
        result[1] = -result[1];
        result[2] = -result[2];
        result[3] = -result[3];
    }

    @Override
    public float computeDistance(float scalar1, float scalar2) {
        return -(scalar1 * scalar2);
    }
}
