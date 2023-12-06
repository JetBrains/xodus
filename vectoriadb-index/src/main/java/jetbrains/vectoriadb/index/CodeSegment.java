package jetbrains.vectoriadb.index;

interface CodeSegment {

    int count();

    int get(int vectorIdx);

    void set(int vectorIdx, int value);

    int maxNumberOfCodes();
}