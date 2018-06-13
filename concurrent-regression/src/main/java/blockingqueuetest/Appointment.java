package blockingqueuetest;

/**
 * Created by admin on 2017/8/8.
 */
public class Appointment<T> {
    private final T patient;

    public Appointment(T t) {
        patient = t;
    }

    public T getPatient() {
        return patient;
    }
}
