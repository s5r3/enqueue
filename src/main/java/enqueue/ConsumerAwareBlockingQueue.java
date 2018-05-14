package enqueue;

import java.util.Arrays;
import java.util.BitSet;

/**
 * @author Sohaib Reza
 */
public class ConsumerAwareBlockingQueue<T> {
    private BitSet ZERO;
    private BitSet ONE;

    private final int capacity;
    private final int consumer;

    private T[] elements;
    private BitSet[] mask;
    private int[] readIndex;
    private int head;
    private int remainingCapacity;

    private final Object writerMonitor = new Object();
    private final Object readerMonitor = new Object();

    public ConsumerAwareBlockingQueue(int capacity, int consumer) {
        ZERO = new BitSet(consumer);
        ONE = new BitSet(consumer);
        ZERO.clear(0, consumer);
        ONE.set(0, consumer);
        this.capacity = capacity;
        this.consumer = consumer;
        this.remainingCapacity = capacity;
        this.elements = (T[]) new Object[capacity];
        this.mask = new BitSet[capacity];
        for (int i = 0; i < capacity; i++) {
            mask[i] = new BitSet(consumer);
            mask[i].set(0, consumer);
        }
        readIndex = new int[consumer];
        Arrays.fill(readIndex, -1);
        this.head = 0;
    }

    public void put(T element) throws InterruptedException {
        synchronized (writerMonitor) {
            while (!mask[head].equals(ONE)) {
                writerMonitor.wait();
            }
            elements[head] = element;
            mask[head].and(ZERO);
            ++head;
            --remainingCapacity;
            if (head == capacity) {
                head = 0;
            }
        }

        synchronized (readerMonitor) {
            readerMonitor.notifyAll();
        }
    }

    public boolean offer(T element, long timeout) throws InterruptedException {
        synchronized (writerMonitor) {
            if (!mask[head].equals(ONE))
                writerMonitor.wait(timeout);

            if (!mask[head].equals(ONE)) {
                return false;
            }

            elements[head] = element;
            mask[head].and(ZERO);
            ++head;
            --remainingCapacity;
            if (head == capacity) {
                head = 0;
            }

        }

        synchronized (readerMonitor) {
            readerMonitor.notifyAll();
        }
        return true;
    }

    public T take(int consumerIndex) throws InterruptedException {
        if (consumerIndex >= consumer) {
            throw new IllegalArgumentException("Maximum consumer index allowed is " + (consumer - 1));
        }

        int index = nextIndexFor(consumerIndex);
        T element;
        synchronized (readerMonitor) {
            while (mask[index].get(consumerIndex)) {
                readerMonitor.wait();
            }
            element = elements[index];
            readIndex[consumerIndex] = index;
            mask[index].set(consumerIndex);
            if (mask[index].equals(ONE))
                ++remainingCapacity;
        }

        synchronized (writerMonitor) {
            writerMonitor.notifyAll();
        }
        return element;
    }

    public T poll(int consumerIndex, long timeout) throws InterruptedException {

        if (consumerIndex >= consumer) {
            throw new IllegalArgumentException("Maximum consumer index allowed is " + (consumer - 1));
        }

        int index = nextIndexFor(consumerIndex);
        T element = null;
        synchronized (readerMonitor) {
            if (mask[index].get(consumerIndex)) {
                System.out.println("waiting");
                readerMonitor.wait(timeout);
            }

            if (mask[index].get(consumerIndex)) {
                return null;
            }

            element = elements[index];
            readIndex[consumerIndex] = index;
            mask[index].set(consumerIndex);
            if (mask[index].equals(ONE))
                ++remainingCapacity;
        }

        synchronized (writerMonitor) {
            writerMonitor.notifyAll();
        }

        return element;
    }

    private synchronized int nextIndexFor(int consumerIndex) {
        int nextIndex = readIndex[consumerIndex] + 1;
        return nextIndex == capacity ? 0 : nextIndex;
    }

    public int remainingCapacity() {
        return remainingCapacity;
    }
}
