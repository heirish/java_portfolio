package com.company.platform.team.projpatternreco.stormtopology.data;

import edu.emory.mathcs.backport.java.util.Arrays;

import java.util.List;

/**
 * Created by admin on 2018/8/8.
 */
public class DBProjectPatternNode {
    private static final String DELIMITER = "¬";
    private int projectId;
    private int patternLevel;
    private String patternKey;
    private String parentKey;
    private String pattern;
    private String represent;

    public DBProjectPatternNode() {

    }
    public DBProjectPatternNode(int projectId, int patternLevel,
                                String patternKey)
    {
       this.projectId = projectId;
       this.patternLevel =  patternLevel;
       this.patternKey = patternKey;
    }

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
    public void setPatternTokens(List<String> tokens) {
        if (tokens != null) {
            this.pattern = String.join("", tokens);
        }
    }

    public String getRepresent() {
        return this.represent;
    }
    public void setRepresent(String represent) {
        this.represent = represent;
    }
    public List<String> getRepresentTokens() {
        if (this.represent != null) {
            return Arrays.asList(this.represent.split(DELIMITER));
        }
        return null;
    }
    public void setReresentTokens(List<String> tokens) {
        if (tokens != null) {
            this.represent = String.join(DELIMITER, tokens);
        }
    }

    public String toString() {
        return String.format("projectId: %s, patternLevel:%s, patternKey: %s, parentKey: %s, pattern: %s, represent:%s",
                projectId, patternLevel, patternKey, parentKey, pattern, represent);
    }
}
