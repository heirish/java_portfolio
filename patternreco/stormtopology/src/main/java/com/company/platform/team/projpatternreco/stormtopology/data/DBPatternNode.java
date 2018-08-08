package com.company.platform.team.projpatternreco.stormtopology.data;

/**
 * Created by admin on 2018/8/8.
 */
public class DBPatternNode {
    private int projectId;
    private int patternLevel;
    private String patternKey;
    private String parentKey;
    private String pattern;

    public int getProjectId() {
        return this.projectId;
    }
    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public int getPatternLevel() {
        return this.patternLevel;
    }
    public void setPatternLevel(int patternLevel) {
        this.patternLevel = patternLevel;
    }

    public String getPatternKey() {
        return this.patternKey;
    }
    public void setPatternKey(String patternKey) {
        this.patternKey = patternKey;
    }

    public String getParentKey() {
        return this.parentKey;
    }
    public void setParentKey(String parentKey) {
        this.parentKey = parentKey;
    }

    public String getPattern(){
        return this.pattern;
    }
    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String toString() {
        return String.format("projectId: %s, patternLevel:%s, patternKey: %s, parentKey: %s, pattern: %s",
                projectId, patternLevel, patternKey, parentKey, pattern);
    }

}
