package blockingqueuetest;

/**
 * Created by admin on 2017/8/8.
 */
public abstract class Pet {
    protected String name;

    public Pet(String name) {
        this.name = name;
    }

    public abstract void examine();
}
