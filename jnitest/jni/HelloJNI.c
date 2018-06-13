#include "com_heirish_jnitest_Hellojni.h"
#include <stdio.h>

JNIEXPORT void JNICALL Java_com_heirish_jnitest_HelloJNI_sayHello
  (JNIEnv * env, jobject object)
{
    printf("HelloWorld");
}