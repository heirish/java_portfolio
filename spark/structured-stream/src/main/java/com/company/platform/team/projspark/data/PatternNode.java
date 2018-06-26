package com.company.platform.team.projspark.data;

import org.apache.commons.lang.StringUtils;
import java.util.List;

/**
 * Created by admin on 2018/6/21.
 */
public class PatternNode {
    private String nodeId;
    private String parentNodeId;
    private List<String> representTokens;

    public PatternNode(String nodeId, List<String> representTokens) {
        this.nodeId = nodeId;
        this.representTokens = representTokens;
    }

    public List<String> getRepresentTokens(){
        return representTokens;
    }

    public void setRepresentTokens(List<String> representTokens){
        this.representTokens = representTokens;
    }

    public void setParentNodeId(String parentNodeId) {
        if (StringUtils.isEmpty(this.parentNodeId)) {
            this.parentNodeId = parentNodeId;
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public String toString() {
        return String.format("nodeId: %s, parentNodeId: %s, representTokens: %s",
                nodeId, parentNodeId, representTokens.toString());
    }
}
