package com.company.platform.team.projspark.common.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.filefilter.RegexFileFilter;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by admin on 2018/7/5.
 */
public class CommonUtils {

    public static File lastNstFileModified(String dir, int nst, String filterRegex) throws Exception{
        if (nst < 1) {
            throw new Exception("nst must greater than 1");
        }
        File choice = null;
        File dirFile = new File(dir);
        FileFilter filter = new RegexFileFilter(filterRegex);
        File[] files = dirFile.listFiles(filter);

        if (files != null && files.length > 0) {
            Arrays.sort(files, LastModifiedFileComparator.LASTMODIFIED_REVERSE);
            choice = files[nst-1];
        }
        return choice;
    }

    public static void moveFiles(String sourceDir, String destDir, String filterRegex) {
        File dirFile = new File(sourceDir);
        FileFilter filter = new RegexFileFilter(filterRegex);
        File[] files = dirFile.listFiles(filter);
        if (files == null) {
            return;
        }

        File destDirFile = new File(destDir);
        for (File file : files) {
            try {
                FileUtils.moveFileToDirectory(file, destDirFile, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // consider the newest files are not completed yet, others can be moved
    //because java 's File.lastModified() precious in milliseconds, but on *nix, it has bug with the last tree digits
    public static int moveFilesAlreadyCompleted(String sourceDir, String destDir, String filterRegex) {
        File dirFile = new File(sourceDir);
        File destDirFile = new File(destDir);
        FileFilter filter = new RegexFileFilter(filterRegex);
        File[] files = dirFile.listFiles(filter);

        int succeedFileCount = 0;
        try {
            File latestFile = lastNstFileModified(sourceDir, 1, filterRegex);

            for (File file : files) {
               if (file.lastModified() < latestFile.lastModified()) {
                   try {
                       FileUtils.moveFileToDirectory(file, destDirFile, true);
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
}
