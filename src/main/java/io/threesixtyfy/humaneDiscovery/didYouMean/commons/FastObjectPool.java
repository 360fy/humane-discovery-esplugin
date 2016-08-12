package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FastObjectPool<T> {

    private final ESLogger logger = Loggers.getLogger(SuggestionsBuilder.class);

    private final Holder<T>[] objects;
    private final int[] ringBuffer;
    private final int size;

    private final AtomicBoolean waitingForRelease = new AtomicBoolean(false);
    private final AtomicInteger takePointer = new AtomicInteger(-1);
    private final AtomicInteger releasePointer = new AtomicInteger(-1);

    private Semaphore semaphore = new Semaphore(1);

    @SuppressWarnings("unchecked")
    public FastObjectPool(PoolFactory<T> factory, int size) {
        this.size = size = getSizePower2(size);

        objects = new Holder[size];
        ringBuffer = new int[size];
        for (int x = 0; x < size; x++) {
            objects[x] = new Holder<>(factory.create(), x);
            ringBuffer[x] = x;
        }

        semaphore.acquireUninterruptibly();
    }

    private int getSizePower2(int size) {
        int newSize = 1;
        while (newSize < size) {
            newSize = newSize << 1;
        }

        return newSize;
    }

    public Holder<T> take() throws InterruptedException {
        // wait when - (takePointer + 1) % size == releasePointer
        int takeSlot = takePointer.incrementAndGet() % size;
        if (takeSlot == releasePointer.get()) {
            // we wait here
            waitingForRelease.set(true);
            semaphore.acquire();
        }

        Holder<T> holder = objects[ringBuffer[takeSlot]];
        if (holder.occupied.compareAndSet(false, true)) {
            return holder;
        }

        logger.error("Take Issue: trying to take already occupied slot: {}", takeSlot);
        return null;
    }

    public void release(Holder<T> object) throws InterruptedException {
        int releaseSlot = releasePointer.incrementAndGet() % size;
        ringBuffer[releaseSlot] = object.slot;

        if (!object.occupied.compareAndSet(true, false)) {
            logger.error("Take Issue: trying to release already freed slot: {}", releaseSlot);
            return;
        }

        if (waitingForRelease.compareAndSet(true, false)) {
            semaphore.release();
        }
    }

    public static class Holder<T> {
        private final T value;
        private final int slot;

        private AtomicBoolean occupied = new AtomicBoolean(false);

        public Holder(T value, int slot) {
            this.value = value;
            this.slot = slot;
        }

        public T getValue() {
            return value;
        }
    }

    public interface PoolFactory<T> {
        T create();
    }
}
