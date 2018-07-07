package com.company.platform.team.projspark.PatternCursoryFinder;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public enum ServiceType {
    SPARK("spark"),
    STORM("storm");

    private String typeString;

    ServiceType(String typeString) {
        this.typeString = typeString;
    }

    public String getTypeString() {
        return typeString;
    }

    public static ServiceType fromString(String text)
            throws IllegalArgumentException{
        for (ServiceType type : ServiceType.values()) {
            if (type.typeString.equalsIgnoreCase(text)) {
                return type;
            }
        }
        throw new IllegalArgumentException("No servicetype with text " + text + " found");
    }

}
