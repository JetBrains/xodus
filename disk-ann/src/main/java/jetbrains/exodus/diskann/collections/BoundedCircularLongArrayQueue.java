package jetbrains.exodus.diskann.collections;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.Semaphore;

public class BoundedCircularLongArrayQueue {
    private static final VarHandle ARRAY_VAR_HANDLE = MethodHandles.arrayElementVarHandle(Element[].class);
    private static final VarHandle TAIL_VAR_HANDLE;
    private static final VarHandle HEAD_VAR_HANDLE;

    static {
        try {
            TAIL_VAR_HANDLE = MethodHandles.lookup().findVarHandle(BoundedCircularLongArrayQueue.class,
                    "tail", long.class);
            HEAD_VAR_HANDLE = MethodHandles.lookup().findVarHandle(BoundedCircularLongArrayQueue.class,
                    "head", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    private volatile long head = 0;

    //prevention of false sharing
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long head_1 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long head_2 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long head_3 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long head_4 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long head_5 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long head_6 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long head_7 = 0;

    @SuppressWarnings("FieldMayBeFinal")
    private volatile long tail = 0;

    //prevention of false sharing
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long tail_1 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long tail_2 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long tail_3 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long tail_4 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long tail_5 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long tail_6 = 0;
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long tail_7 = 0;

    private final Element[] array;

    private boolean stop;

    private final Semaphore readersWaitingSemaphore = new Semaphore(0);

    public BoundedCircularLongArrayQueue(int capacity) {
        capacity = Integer.highestOneBit(capacity - 1) << 1;

        //ensure absence of false sharing
        array = new Element[capacity];
        for (int i = 0; i < array.length; i++) {
            array[i] = new Element(i);
        }
    }

    public void stop() {
        stop = true;
        readersWaitingSemaphore.release(Integer.MAX_VALUE / 2);
    }

    public void add(long value) {
        Element element;
        var tail = this.tail;

        while (true) {
            element = (Element) ARRAY_VAR_HANDLE.getVolatile(array, arrayIndex((int) tail));
            var pos = element.pos;

            var diff = pos - tail;
            if (diff == 0) {
                if (TAIL_VAR_HANDLE.compareAndSet(this, tail, tail + 1)) {
                    break;
                }
            } else if (diff < 0) {
                return;
            } else {
                tail = this.tail;
            }
        }

        element.value = value;
        element.pos = tail + 1;

        readersWaitingSemaphore.release();
    }

    private int arrayIndex(int elementIndex) {
        return elementIndex & (array.length - 1);
    }

    public long dequeue() throws InterruptedException {
        readersWaitingSemaphore.acquire();

        var head = this.head;
        Element element;

        while (true) {
            element = (Element) ARRAY_VAR_HANDLE.getVolatile(array, arrayIndex((int) head));

            var pos = element.pos;

            var diff = pos - (head + 1);
            if (diff == 0) {
                if (HEAD_VAR_HANDLE.compareAndSet(this, head, head + 1)) {
                    break;
                }
            } else if (diff < 0) {
                if (stop) {
                    return -1;
                }
            } else {
                head = this.head;
            }
        }

        var value = element.value;
        element.pos = head + array.length;

        return value;
    }

    private static final class Element {
        private volatile long pos;
        private long value;

        private Element(long pos) {
            this.pos = pos;
        }

        //padding to prevent false sharing
        @SuppressWarnings({"FieldMayBeFinal", "unused"})
        private volatile long value_1;
        @SuppressWarnings({"FieldMayBeFinal", "unused"})
        private volatile long value_2;
        @SuppressWarnings({"FieldMayBeFinal", "unused"})
        private volatile long value_3;
        @SuppressWarnings({"FieldMayBeFinal", "unused"})
        private volatile long value_4;
        @SuppressWarnings({"FieldMayBeFinal", "unused"})
        private volatile long value_5;
        @SuppressWarnings({"FieldMayBeFinal", "unused"})
        private volatile long value_6;
    }
}
