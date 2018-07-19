package com.company.platform.team.projpatternreco.common.utils;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by Administrator on 2018/7/8 0008.
 */
public class HdfsUtil {
    public static FileStatus lastNstModified(String dir, int nst, String filterRegex, String resourcefile) throws Exception{
        if (nst < 1) {
            throw new Exception("nst must greater than 1");
        }

        Configuration conf = new Configuration();
        conf.set("file.pattern", StringEscapeUtils.escapeJava(filterRegex));
        if (!StringUtils.isEmpty(resourcefile)) {
            conf.addResource(new Path(resourcefile));
        }
        FileSystem fs = FileSystem.get(conf);
        RegexPathFilter pathFilter = new RegexPathFilter();
        pathFilter.setConf(conf);
        FileStatus[] status = fs.listStatus(new Path(dir), pathFilter);

        FileStatus choice = null;
        if (status !=null && status.length >0) {
            Arrays.sort(status, new Comparator<FileStatus>() {
                public int compare(FileStatus o1, FileStatus o2) {
                    // Intentional: Reverse order for this demo
                    return Long.valueOf(o2.getModificationTime())
                            .compareTo(Long.valueOf(o1.getModificationTime()));
                }
            });
            choice = status[nst-1];
        }
        return choice;
    }

    public static int moveFilesAlreadyCompleted(String sourceDir, String destDir,
                                                String filterRegex, String resourcefile) {
        Configuration conf = new Configuration();
        conf.set("file.pattern", StringEscapeUtils.escapeJava(filterRegex));
        if (!StringUtils.isEmpty(resourcefile)) {
            conf.addResource(new Path(resourcefile));
        }
        RegexPathFilter pathFilter = new RegexPathFilter();
        pathFilter.setConf(conf);

        int succeedFileCount = 0;
        try {
            FileStatus latestFile = lastNstModified(sourceDir, 1, filterRegex, resourcefile);
            System.out.println(latestFile.getPath().toString());
            FileSystem destfs= FileSystem.get(conf);
            Path destPath = new Path(destDir);
            if (!destfs.exists(destPath)) {
                destfs.mkdirs(destPath);
            }

            FileSystem srcfs = FileSystem.get(conf);
            FileStatus[] status = srcfs.listStatus(new Path(sourceDir), pathFilter);
            if (!destfs.isDirectory(destPath)
                    || latestFile == null
                    || status == null) {
                return 0;
            }

            for (FileStatus file : status) {
                if (file.getModificationTime() < latestFile.getModificationTime()) {
                    try {
                        FileUtil.copy(srcfs, file.getPath(),
                                destfs, destPath,
                                true, true, conf);
                        succeedFileCount++;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return succeedFileCount;
    }

    public static boolean uploadLocalFile2HDFS(String localFile, String hdfsFile)
            throws IOException {
        if (StringUtils.isBlank(localFile) || StringUtils.isBlank(hdfsFile)) {
            return false;
        }
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(hdfsFile), config);
        Path src = new Path(localFile);
        Path dst = new Path(hdfsFile);
        hdfs.copyFromLocalFile(src, dst);
        hdfs.close();
        return true;
    }

    public static boolean deleteDir(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return false;
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dir), conf);
        fs.delete(new Path(dir), true);
        fs.close();
        return true;
    }
}
