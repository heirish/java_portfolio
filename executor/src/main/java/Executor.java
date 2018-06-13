import com.sun.org.apache.xml.internal.security.utils.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.io.*;
import java.nio.file.*;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created on 2017/8/14.
 *
 * @author heirishlee@gmail.com
 **/
public class Executor {
    public static void main(String[] argv) {
        try {
            String s = null;
            Process p = Runtime.getRuntime().exec("java -jar E:\\my_study_place\\learnjava\\executor\\target\\maven-helloworld-1.0-SNAPSHOT.jar");
            //synchronized (p) {
                p.waitFor();
            //}
            System.out.println(p.exitValue());
            if (p.exitValue() != 0) {
                BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                while ((s = stdError.readLine()) != null) {
                    System.out.println(s);
                }
            } else {
                BufferedReader stdOut = new BufferedReader(new InputStreamReader(p.getInputStream()));
                while ((s = stdOut.readLine()) != null) {
                    System.out.print(s);
                }
            }

            //tmpFileTest();
            //hashSetTest();
            //recursiveSearchFileTest();
            //recursiveSearchFileTest1("E:\\vmware_share\\sampleApp");
            Path filePath = Paths.get("E:\\vmware_share\\sampleApp");
            //合并两个路径的技术允许你先定义一个固定的根目录然后再附上局部的路径
            System.out.println(filePath.resolve("a.txt"));
            Path test = filePath.resolve("test\\b.txt");
            System.out.println(test);
            //获取相对路径
            System.out.println(filePath.relativize(test));

            //regexTest();
            //synchronizeDirectoryTest("E:\\vmware_share\\sampleApp", "E:\\tmp");
            String encoded = encodeBase64Test("helloworld");
            System.out.println(encoded);

            //object
            Executor exec = new Executor();
            exec.objectTest();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void tmpFileTest() throws IOException
    {
        //create temp file
        byte[] writeContent = new byte[]{0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F,
                0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7A};
        Path filePath = Paths.get("E:\\my_study_place\\learnjava\\executor\\target");
        Path tempFile = Files.createTempFile(filePath, "heirish_", "_a.txt");
        tempFile.toFile().setReadable(true, false);
        tempFile.toFile().setWritable(true, false);
        tempFile.toFile().setExecutable(true, false);
        Files.write(tempFile,writeContent);
    }

    private static void kafkaTest()
    {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "abcd");
    }

    private static void hashSetTest()
    {
        //HashSet Test
        HashSet<String> paths = new HashSet<String>();
        paths.add("E:\\Tmp");
        paths.add("E:\\Tmp");
        paths.add("D:\\Tmp");
        String strPaths = StringUtils.join(paths.toArray(), " ");
        System.out.println(strPaths);
    }

    private void objectTest() {
        System.out.println(this.getClass());
        System.out.println(this.getClass().getResourceAsStream("/test"));
    }

    private static void recursiveSearchFileTest() {
        String symbolPath = "E:";
        String symbolFileID = "0E72C1BE0FB74FDD8611D84983948BA01";

        String fileDir = symbolPath.endsWith(File.separator) ? symbolPath : symbolPath + File.separator;
        System.out.println(fileDir);
        File dir = new File(fileDir);
        String[] fileExtension = {"sym", "SYM"};
        Collection<File> files = FileUtils.listFiles(dir, fileExtension,true);

        if (files.size() != 0) {
            for (File file : files) {
                System.out.println(file.getAbsolutePath());
                System.out.println(file.getAbsoluteFile());
                if (file.getAbsolutePath().endsWith(symbolFileID + File.separator)) {
                    System.out.println("Found you!");
                } else {
                    System.out.println("Where are you!");
                }
            }
        } else {
            System.out.println("no listed files from symbolPath");
        }
    }

    private static void recursiveSearchFileTest1(String sourcePath) throws Exception {
        final Path remotePath = Paths.get(sourcePath);
        EnumSet<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);

        if (Files.exists(remotePath, LinkOption.NOFOLLOW_LINKS)) {
            FileFinder finder = new FileFinder("*.sym");
            Files.walkFileTree(remotePath, options, 10, finder);
            for(Path path : finder.getMatchedPaths()) {
                System.out.println("Found file:" + path);
            }
        }
    }

    private static void synchronizeDirectoryTest(String sourcePath, String destPath) throws Exception {
        final Path remotePath = Paths.get(sourcePath);
        final Path localPath = Paths.get(destPath);
        EnumSet<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);

        if (Files.exists(remotePath, LinkOption.NOFOLLOW_LINKS)) {
            if (!Files.exists(localPath, LinkOption.NOFOLLOW_LINKS)) {
                Files.createDirectories(localPath);
            }
            Files.walkFileTree(remotePath, options,10, new DirectorySynchronizer(remotePath, localPath));
        }
    }

    private static void regexTest() {
        Pattern pattern = Pattern.compile("/\\d{10}");
        String testString = "/homedir/dir1/dir2/1234567890/abcde";
        Matcher matcher = pattern.matcher(testString);
        if (matcher.find()) {
            System.out.println(matcher.group());
            testString = testString.replace(matcher.group(), "");
            System.out.println(testString);
        }
    }

    private static String encodeBase64Test(String text) {
        String retEncoded = null;
        try {
            String tmpText = new String(text.getBytes(), "UTF-8");
            retEncoded = Base64.encode(tmpText.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retEncoded;
    }

}