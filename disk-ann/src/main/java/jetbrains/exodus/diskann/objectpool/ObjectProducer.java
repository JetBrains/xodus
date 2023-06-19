package jetbrains.exodus.diskann.objectpool;

public interface ObjectProducer<T> {
    T produce();

    void clear(T t);
}
