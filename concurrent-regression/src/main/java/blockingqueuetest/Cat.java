package blockingqueuetest;

/**
 * Created by admin on 2017/8/8.
 */
public class Cat extends Pet{
    public Cat(String name) {
        super(name);
    }

    public void examine() {
        System.out.println(String.format("Cat [%s] meom.", name));
    }
}
