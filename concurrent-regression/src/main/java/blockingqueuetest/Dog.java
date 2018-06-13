package blockingqueuetest;

/**
 * Created by admin on 2017/8/8.
 */
public class Dog extends Pet{
    public Dog(String name) {
        super(name);
    }

    public void examine() {
        System.out.println(String.format("Dog [%s] Woof.", name));
    }
}
