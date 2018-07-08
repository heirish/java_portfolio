package com.company.platform.team.projspark.common.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2018/6/30 0030.
 */
public class RegexPathFilter extends Configured implements PathFilter {
    Pattern pattern;
    Configuration conf;
    FileSystem fs;

    @Override
    public boolean accept(Path path) {
        try {
            if (StringUtils.equalsIgnoreCase(conf.get("file.keepdir"), "true")
                && fs.isDirectory(path)) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        Matcher m = pattern.matcher(path.toString());
        System.out.println("Is path:" + path.toString() + " matching "
        + conf.get("file.pattern") + " ?, " + m.matches());
        return m.matches();
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        if (conf != null) {
            try {
                String filePattern = conf.get("file.pattern");
                if (filePattern == null || StringUtils.isEmpty(filePattern)) {
                    filePattern = ".*"; // Every files by default
                    conf.set("file.pattern", filePattern);
                }
                //System.out.println(conf.toString());
                //System.out.println(conf.getRaw("fs.defaultFS"));
                //System.out.println(conf.getRaw("hdfs-site.xml"));
                pattern = Pattern.compile(filePattern);
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
