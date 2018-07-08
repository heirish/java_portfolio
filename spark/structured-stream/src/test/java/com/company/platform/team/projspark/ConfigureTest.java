package com.company.platform.team.projspark;

import com.company.platform.team.projspark.PatternCursoryFinder.FinderServiceConfigure;
import com.company.platform.team.projspark.PatternRefiner.RefinerServiceConfigure;
import com.google.gson.Gson;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class ConfigureTest {
    private static final Gson gson = new Gson();
    @Test
    public void RefinerConfTest() {
        try {
            FinderServiceConfigure conf = FinderServiceConfigure.parseFromJson("conf/cursoryfinder.json");
            System.out.println(conf.getLogOutType());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
