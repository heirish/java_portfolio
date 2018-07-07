package com.company.platform.team.projspark.PatternNodeHelper;

import com.company.platform.team.projspark.PatternCursoryFinder.ServiceType;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public enum PatternNodeCenterType {
    HDFS("hdfs"),
    HBASE("hbase"),
    REDIS("redis"),
    DATABASE("database");

    private String typeString;

    PatternNodeCenterType(String typeString) {
        this.typeString = typeString;
    }

    public String getTypeString() {
        return typeString;
    }

    public static PatternNodeCenterType fromString(String text)
            throws IllegalArgumentException{
        for (PatternNodeCenterType type : PatternNodeCenterType.values()) {
            if (type.typeString.equalsIgnoreCase(text)) {
                return type;
            }
        }
        throw new IllegalArgumentException("No PatternNodeCenterType with text " + text + " found");
    }
}
