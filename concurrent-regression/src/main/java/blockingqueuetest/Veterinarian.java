package blockingqueuetest;

import java.util.concurrent.BlockingQueue;

/**
 * Created by admin on 2017/8/8.
 */
public class Veterinarian extends Thread{
    private final BlockingQueue<Appointment<Pet>> appts;
    private final int breakTime;
    private boolean shutDown = false;

    public Veterinarian(BlockingQueue<Appointment<Pet>> app, int breakTime) {
        this.appts = app;
        this.breakTime = breakTime;
    }

    public synchronized void shutDown() {
        shutDown = true;
    }

    @Override
    public void run() {
        while (!shutDown) {
            seePatient();
            try {
                Thread.sleep(breakTime);
            } catch (Exception e) {
                shutDown = true;
            }
        }
    }

    public  void seePatient() {
        try {
            Appointment<Pet> appt = appts.take();
            Pet pet = appt.getPatient();
            System.out.println(String.format("Pet examine in thread:[%d].", currentThread().getId()));
            pet.examine();
        } catch (Exception e) {
            shutDown = true;
        }
    }
}
