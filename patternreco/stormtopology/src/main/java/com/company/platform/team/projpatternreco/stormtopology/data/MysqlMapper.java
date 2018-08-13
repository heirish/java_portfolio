package com.company.platform.team.projpatternreco.stormtopology.data;

import java.util.List;

/**
 * Created by admin on 2018/8/8.
 */
public interface MysqlMapper {
    void deleteProjectNodes(int projectId);
    int insertProjectNodes(List<DBProjectPatternNode> nodes);
    List<DBProjectPatternNode> selectProjectLeaves(String projectName);
    int updateParentNode(int projectId, int patternLevel, String patternKey, String parentKey);

    List<DBProject> selectProjectMetas();
    DBProject selectProjectMeta(String projectName);
}
