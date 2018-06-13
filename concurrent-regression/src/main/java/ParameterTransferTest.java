/**
 * Created by admin on 2017/8/8.
 */
public class ParameterTransferTest {
    public static void main(String[] argv) {
        StringBuffer s1 = new StringBuffer("s1");
        StringBuffer s2 = new StringBuffer("s2");
        test(s1, s2);
        System.out.println(s1);
        System.out.println(s2);

        String str = "0x1234234";
        subStringTest(str);
        //System.out.println(str);
    }

    private static void test(StringBuffer s1, StringBuffer s2) {
        System.out.println(s1);
        System.out.println(s2);

        s2 = s1; //此时s2和原s1的引用值相同
        s1 = new StringBuffer("new s1"); //s1的引用值指向新的内存
        System.out.println(s1);
        System.out.println(s2);

        s1.append("append"); //append到"new s1"指向的内存
        s2.append("append"); //append到原s1指向的内存
    }

    private static void subStringTest(String str) {
        //此时的str己不是main中的str, 引用己经进行了复制，只是两个引用指向同一块内存
        if (str.startsWith("0x") || str.startsWith("0X")) {
            System.out.println(str);
            str = str.substring(2); //而在此处，str.substring是另外一块内存, 所以并不会对main中的str有什么影响
            System.out.println(str);
        }
    }
}
