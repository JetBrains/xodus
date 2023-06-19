package jetbrains.exodus.diskann.objectpool;

import org.jctools.queues.MpmcUnboundedXaddArrayQueue;

public class UnboundedObjectPool<T> {
    private final ObjectProducer<T> producer;
    private final MpmcUnboundedXaddArrayQueue<T> queue =
            new MpmcUnboundedXaddArrayQueue<>(4 * Runtime.getRuntime().availableProcessors());

    public UnboundedObjectPool(ObjectProducer<T> producer) {
        this.producer = producer;
    }

    public T borrowObject() {
        T t = queue.relaxedPoll();
        if (t == null) {
            t = producer.produce();
        }
        return t;
    }

    public void returnObject(T t) {
        producer.clear(t);
        queue.offer(t);
    }
}
