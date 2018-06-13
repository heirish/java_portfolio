package blockingqueuetest;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by admin on 2017/8/8.
 */
public class VeterinarianTest {
    private static final int MAX_APPOINTS = 100;
    private final BlockingQueue<Appointment<Pet>> appts =
            new ArrayBlockingQueue<Appointment<Pet>>(MAX_APPOINTS);

    public static void main(String[] argv) {
        VeterinarianTest test = new VeterinarianTest();
        test.initAppointments();

        Veterinarian vet1 = new Veterinarian(test.appts, 10);
        Veterinarian vet2 = new Veterinarian(test.appts, 20);
        Veterinarian vet3 = new Veterinarian(test.appts, 30);

        vet1.start();
        vet2.start();
        vet3.start();
    }

    private void initAppointments() {
       appts.add(new Appointment<Pet>(new Cat("cat1")));
       appts.add(new Appointment<Pet>(new Cat("cat2")));
       appts.add(new Appointment<Pet>(new Dog("Dog1")));
       appts.add(new Appointment<Pet>(new Cat("cat4")));
       appts.add(new Appointment<Pet>(new Dog("Dog2")));
       appts.add(new Appointment<Pet>(new Dog("Dog3")));
       appts.add(new Appointment<Pet>(new Dog("Dog4")));
       appts.add(new Appointment<Pet>(new Cat("cat3")));
       appts.add(new Appointment<Pet>(new Dog("Dog5")));
       appts.add(new Appointment<Pet>(new Dog("Dog6")));
    }
}
