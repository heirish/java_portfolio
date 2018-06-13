package com.heirish.jnitest;

/**
 * Created by admin on 2017/10/11.
 */
public class HelloJNI {
    static {
        System.loadLibrary("HelloJNI");
    }

    private native void sayHello();

    public static void main(String[] args) {
        new HelloJNI().sayHello();
    }
}
