package com.company.platform.team.projpatternreco.sparkandhadoop;

import com.company.platform.team.projpatternreco.sparkandhadoop.patterncursoryfinder.FinderServiceConfigure;
import com.google.gson.Gson;
import org.junit.Test;

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
