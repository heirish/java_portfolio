package com.company.platform.team.projspark.data;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 2018/6/21.
 */
public class PatternNodeBak {
    private List<String> representTokens;
    private List<String> patternTokens;

    private PatternNodeBak parent;
    private List<PatternNodeBak> children;

    public PatternNodeBak(List<String> representTokens) {
        this.representTokens = representTokens;
        this.patternTokens = representTokens;

        this.parent = null;
        this.children = null;
    }


    public boolean hasParent() {
        return parent != null;
    }

    public boolean hasChildren() {
       return (children != null && children.size() > 0);
    }

    public void setParent(PatternNodeBak node){
        parent = node;
    }

    public void addChildren(PatternNodeBak node) {
       if (children == null) {
           children = new ArrayList<>();
       }
       children.add(node);
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
