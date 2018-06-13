public class SynchronizedTest {
    private int counter = 0;

    public static void main(String[] argv){
        //TODO:multi threads
        final SynchronizedTest test = new SynchronizedTest();
        final int MAX_RUNTIMES = 100;

        new Thread(new Runnable() {
            public void run(){
                int runTimes = 0;
                try {
                    while (runTimes++ < MAX_RUNTIMES) {
                        test.decreaseCounter();
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        new Thread(new Runnable() {
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
    }

    private synchronized void increaseCounter() {
        counter ++;
        System.out.println(String.format("In addCounter: threadid=[%d], counter=[%d].",
                Thread.currentThread().getId(), counter));
    }

    private synchronized void decreaseCounter() {
        counter --;
        System.out.println(String.format("In addCounter: threadid=[%d], counter=[%d].",
                Thread.currentThread().getId(), counter));
    }

}
