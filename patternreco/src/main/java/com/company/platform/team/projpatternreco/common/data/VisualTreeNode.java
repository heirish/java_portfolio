package com.company.platform.team.projpatternreco.common.data;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 2018/7/5.
 */
public class VisualTreeNode {
    String data;
    String nodeId;
    List<VisualTreeNode> children;

    public VisualTreeNode(String nodeId, String data) {
        this.nodeId = nodeId;
        this.data= data;
        this.children = new ArrayList<>();
    }

    public void addChild(VisualTreeNode child) {
        this.children.add(child);
    }

    public VisualTreeNode getNode(String nodeId) {
        if (StringUtils.equals(this.nodeId, nodeId)) {
            return this;
        } else {
            for (VisualTreeNode node : children) {
                VisualTreeNode foundNode = node.getNode(nodeId);
                if (foundNode != null) {
                    return foundNode;
                }
            }
        }
        return null;
    }

    public String visualize() {
        return print("", true);
    }

    private String print(String prefix, boolean isTail) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(prefix + (isTail ? "└── " : "├── ") + data);
        stringBuilder.append(System.getProperty("line.separator"));
        for (int i = 0; i < children.size() - 1; i++) {
            stringBuilder.append(children.get(i).print(prefix + (isTail ? "    " : "│   "), false));
        }
        if (children.size() > 0) {
            stringBuilder.append(children.get(children.size() - 1)
                    .print(prefix + (isTail ?"    " : "│   "), true));
        }
        return stringBuilder.toString();
    }
}
