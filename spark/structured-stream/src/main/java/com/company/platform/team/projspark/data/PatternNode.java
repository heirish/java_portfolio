package com.company.platform.team.projspark.data;

import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Created by admin on 2018/6/21.
 */
public class PatternNode {
    private List<String> representTokens;
    private List<String> patternTokens;

    private PatternNodeKey parentNodeKey;
    // private PatternNodeBak parent;
    // private List<PatternNodeBak> children;

    public PatternNode(List<String> representTokens) {
        this.representTokens = representTokens;
        this.patternTokens = representTokens;
    }

    public PatternNode(PatternNode node) {
       this.representTokens = node.representTokens;
       this.parentNodeKey = node.parentNodeKey;
       this.patternTokens = node.patternTokens;
    }

    public boolean hasParent() {
        return parentNodeKey != null;
    }

    public void setParent(PatternNodeKey nodeKey){
        this.parentNodeKey = nodeKey;
    }

    public PatternNodeKey getParentId() {
        return this.parentNodeKey;
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
