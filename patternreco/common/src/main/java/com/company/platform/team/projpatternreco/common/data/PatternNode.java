package com.company.platform.team.projpatternreco.common.data;

import com.google.gson.Gson;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/6/21.
 */
public final class PatternNode {
    private static final String DELIMITER = "#@#";
    private static final String MEMBER_NAME_PATTERN = "patternTokens";
    private static final String MEMBER_NAME_REPRESENT = "representTokens";
    private static final String MEMBER_NAME_PARENTKEY = "parentNodeKey";
    private static final String MEMBER_NAME_TIMESTAMP = "lastupdatedTime";

    private static final Gson gson = new Gson();
    private static final Logger logger = LoggerFactory.getLogger(PatternNode.class);

    private List<String> representTokens;
    private List<String> patternTokens;
    private PatternNodeKey parentNodeKey;
    private long lastupdatedTime;

    public PatternNode(List<String> representTokens) {
        this.representTokens = representTokens;
        this.patternTokens = representTokens;
    }

    public PatternNode(PatternNode node) {
       this.representTokens = node.representTokens;
       this.parentNodeKey = node.parentNodeKey;
       this.patternTokens = node.patternTokens;
    }

    public void setLastupdatedTime(long updateTime) {
        this.lastupdatedTime = updateTime;
    }

    public long getLastupdatedTime() {
        return lastupdatedTime;
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

    public String toJson() {
        HashMap<String, String> nodeMembers = new HashMap<>();
        nodeMembers.put(MEMBER_NAME_PATTERN, String.join(DELIMITER, patternTokens));
        nodeMembers.put(MEMBER_NAME_REPRESENT, String.join(DELIMITER, representTokens));
        nodeMembers.put(MEMBER_NAME_TIMESTAMP, String.valueOf(lastupdatedTime));
        if (parentNodeKey != null) {
            nodeMembers.put(MEMBER_NAME_PARENTKEY, parentNodeKey.toString());
        } else {
            nodeMembers.put(MEMBER_NAME_PARENTKEY, "");
        }
        return gson.toJson(nodeMembers);
    }

    public static PatternNode fromJson(String json) throws Exception {
        Map<String, String> nodeMembers = gson.fromJson(json, Map.class);
        List<String> patternTokens = Arrays.asList(nodeMembers.get(MEMBER_NAME_PATTERN).split(DELIMITER));
        List<String> representTokens = Arrays.asList(nodeMembers.get(MEMBER_NAME_REPRESENT).split(DELIMITER));
        long lastupdatedTime = Long.parseLong(nodeMembers.get(MEMBER_NAME_TIMESTAMP));
        String parentNodeKey = nodeMembers.get(MEMBER_NAME_PARENTKEY);

        PatternNode node = new PatternNode(representTokens);
        node.patternTokens = patternTokens;
        node.lastupdatedTime = lastupdatedTime;
        if (!StringUtils.isBlank(parentNodeKey)) {
            node.parentNodeKey = PatternNodeKey.fromString(parentNodeKey);
        } else {
            node.parentNodeKey = null;
        }
        return node;
    }

    public boolean equals(PatternNode o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (this.getClass() != o.getClass()) {
            return false;
        }

        if (this.hasParent() != o.hasParent()) {
            return false;
        }
        if (this.hasParent() && !this.parentNodeKey.equals(o.getParentId())) {
            return false;
        }

        return this.patternTokens.equals(o.getPatternTokens())
                && this.representTokens.equals((o.getRepresentTokens()));
    }
}
