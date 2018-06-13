import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by admin on 2017/8/8.
 */
public class LockTest {
    private int counter = 0;
    private final Lock lock = new ReentrantLock();
    public static void main(String[] argv) {
        final LockTest test = new LockTest();
        final int MAX_RUNTIMES = 20;

        new Thread(new Runnable() {
            @Override
            public void run() {
               int runTimes = 0;
               try {
                   while (runTimes++ < MAX_RUNTIMES) {
                       test.decreaseCounter();
                       Thread.sleep(1);
                   }
               } catch (Exception e) {
                   e.printStackTrace();
               }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                int runTimes = 0;
                try {
                    while (runTimes++ < MAX_RUNTIMES) {
                        test.increaseCounter();
                        Thread.sleep(2);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        Thread  thread3 = new Thread() {
            public void run() {
                int runTimes = 0;
                try {
                    while (runTimes++ < MAX_RUNTIMES) {
                        test.increaseCounter();
                        Thread.sleep(2);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        thread3.start();
    }

    private void decreaseCounter() {
        lock.lock();
        counter--;
        System.out.println(String.format("decreaseCounter: threadid [%d], counter [%d]",
                Thread.currentThread().getId(), counter));
        lock.unlock();
    }

    private void increaseCounter() {
        lock.lock();
        counter++;
        System.out.println(String.format("decreaseCounter: threadid [%d], counter [%d]",
                Thread.currentThread().getId(), counter));
        lock.unlock();
    }

}
