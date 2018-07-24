package com.company.platform.team.projpatternreco.stormtopology.utils;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public enum RunningType {
    LOCAL("local"),
    CLUSTER("cluster");

    private String typeString;

    RunningType(String typeString) {
        this.typeString = typeString;
    }

    public String getTypeString() {
        return typeString;
    }

    public static RunningType fromString(String text)
            throws IllegalArgumentException{
        for (RunningType type : RunningType.values()) {
            if (type.typeString.equalsIgnoreCase(text)) {
                return type;
            }
        }
        throw new IllegalArgumentException("No running type with text " + text + " found");
    }
}
