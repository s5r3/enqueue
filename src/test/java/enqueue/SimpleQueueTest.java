package enqueue;

/**
 * @author Nur Alam Zico
 */
public class SimpleQueueTest {
    private static int CONSUMER = 2;
    private static int CAPACITY = 10;
    private static int MAX_GEN = 50;
    private ConsumerAwareBlockingQueue<String> testQueue;
    private Thread producer;
    private Thread[] consumer;

    public static void main(String... args) {
        new SimpleQueueTest().run();
    }

    public SimpleQueueTest() {
        this.testQueue = new ConsumerAwareBlockingQueue<>(CAPACITY, CONSUMER);
        this.producer = new Thread(() -> {
            int count = 0;
            for (int i = 0; i < MAX_GEN; i++) {
                try {
                    Thread.sleep(10);
                    boolean r = testQueue.offer("lalala-" + i, 200);
                    if (r) count++;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("producer wrote " + count + " data");
            System.out.println("producer exit...");
        });

        this.consumer = new Thread[CONSUMER];
        for (int i = 0; i < CONSUMER; i++) {
            int finalI = i;
            this.consumer[i] = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(200 * (finalI + 2));
                        String d = testQueue.poll(finalI, 200);
                        if (d == null) break;
                        System.out.println("Thread : " + Thread.currentThread().getName() + ", data: " + d);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("consumer-" + finalI + " exit!");
                System.out.println("[consumer-" + finalI + "] Queue free slot: " + testQueue.remainingCapacity());
            }, "consumer-" + i);
        }
    }

    public void run() {
        producer.start();
        for (int i = 0; i < CONSUMER; i++) {
            consumer[i].start();
        }

        try {
            producer.join();
            for (int i = 0; i < CONSUMER; i++) {
                consumer[i].join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
