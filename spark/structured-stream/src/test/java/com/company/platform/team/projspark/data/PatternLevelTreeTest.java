package com.company.platform.team.projspark.data;

import org.junit.Test;

/**
 * Created by Administrator on 2018/7/6 0006.
 */
public class PatternLevelTreeTest {
    @Test
    public void saveTreeToFileTest(){
        PatternLevelTree.getInstance().saveTreeToFile("./LevelTreeSaveTest");
    }

    @Test
    public void backupTreeTest(){
        PatternLevelTree.getInstance().backupTree("./LevelTreebakTest");
    }
}
