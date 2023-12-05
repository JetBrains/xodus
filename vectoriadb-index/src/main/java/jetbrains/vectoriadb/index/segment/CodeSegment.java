package jetbrains.vectoriadb.index.segment;

public interface CodeSegment {

    int count();

    int get(int vectorIdx);

    void set(int vectorIdx, int value);

    int maxNumberOfCodes();
}