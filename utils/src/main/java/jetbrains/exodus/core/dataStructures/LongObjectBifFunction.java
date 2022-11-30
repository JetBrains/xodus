package jetbrains.exodus.core.dataStructures;

@FunctionalInterface
public interface LongObjectBifFunction<T, U> {
    U apply(long first, T second);
}
