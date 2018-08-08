package com.company.platform.team.projpatternreco.stormtopology.data;

import java.util.List;

/**
 * Created by admin on 2018/8/8.
 */
public interface DBPatternNodeMapper {
    List<DBPatternNode> selectProjectLeaves(String projectName);
}
