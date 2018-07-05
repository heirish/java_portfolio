package com.company.platform.team.projspark.data;

import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/6/21.
 */
public class PatternNode {
    private List<String> representTokens;
    private List<String> patternTokens;

    private String parentNodeId;
    // private PatternNodeBak parent;
    // private List<PatternNodeBak> children;

    public PatternNode(List<String> representTokens) {
        this.representTokens = representTokens;
        this.patternTokens = representTokens;
        this.parentNodeId = "";
    }

    public PatternNode(PatternNode node) {
       this.representTokens = node.representTokens;
       this.parentNodeId = node.parentNodeId;
       this.patternTokens = node.patternTokens;
    }

    public boolean hasParent() {
        return !StringUtils.isEmpty(parentNodeId);
    }

    public void setParent(String nodeId){
        this.parentNodeId = nodeId;
    }

    public String getParentId() {
        return this.parentNodeId;
    }

    public List<String> getRepresentTokens(){
        return representTokens;
    }

    public List<String> getPatternTokens() {
        return patternTokens;
    }

    public void updatePatternTokens(List<String> patternTokens) {
       this.patternTokens = patternTokens;
    }

    public String toString() {
        return String.format("represent: %s, pattern: %s",
                String.join("", representTokens),
                String.join("", patternTokens));
    }
}
