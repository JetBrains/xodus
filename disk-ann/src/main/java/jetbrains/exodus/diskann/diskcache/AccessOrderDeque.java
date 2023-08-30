package jetbrains.exodus.diskann.diskcache;

import org.jetbrains.annotations.Nullable;

import java.util.Deque;

final class AccessOrderDeque<E extends AccessOrderDeque.AccessOrder<E>> extends AbstractLinkedDeque<E> {

    @Override
    public boolean contains(Object o) {
        return (o instanceof AccessOrder<?>) && contains((AccessOrder<?>) o);
    }

    // A fast-path containment check
    boolean contains(AccessOrder<?> e) {
        return (e.getPreviousInAccessOrder() != null)
                || (e.getNextInAccessOrder() != null)
                || (e == first);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
        return (o instanceof AccessOrder<?>) && remove((E) o);
    }

    // A fast-path removal
    boolean remove(E e) {
        if (contains(e)) {
            unlink(e);
            return true;
        }
        return false;
    }

    @Override
    public @Nullable E getPrevious(E e) {
        return e.getPreviousInAccessOrder();
    }

    @Override
    public void setPrevious(E e, @Nullable E prev) {
        e.setPreviousInAccessOrder(prev);
    }

    @Override
    public @Nullable E getNext(E e) {
        return e.getNextInAccessOrder();
    }

    @Override
    public void setNext(E e, @Nullable E next) {
        e.setNextInAccessOrder(next);
    }

    /**
     * An element that is linked on the {@link Deque}.
     */
    interface AccessOrder<T extends AccessOrder<T>> {

        /**
         * Retrieves the previous element or <tt>null</tt> if either the element is unlinked or the
         * first element on the deque.
         */
        @Nullable
        T getPreviousInAccessOrder();

        /** Sets the previous element or <tt>null</tt> if there is no link. */
        void setPreviousInAccessOrder(@Nullable T prev);

        /**
         * Retrieves the next element or <tt>null</tt> if either the element is unlinked or the last
         * element on the deque.
         */
        @Nullable T getNextInAccessOrder();

        /** Sets the next element or <tt>null</tt> if there is no link. */
        void setNextInAccessOrder(@Nullable T next);
    }
}
