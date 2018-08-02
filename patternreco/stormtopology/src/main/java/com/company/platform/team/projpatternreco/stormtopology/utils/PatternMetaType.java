package com.company.platform.team.projpatternreco.stormtopology.utils;

/**
 * Created by admin on 2018/7/31.
 */
public enum PatternMetaType {
    LEAF_SIMILARITY_MIN("leafSimilarityMin"),
    LEAF_SIMILARITY_MAX("leafSimilarityMax"),
    LEAF_NODES_LIMIT("leafNodesLimit"),
    DECAY_FACTOR("decayFactor"),
    FIND_TOLERANCE("findTolerance"),
    BODY_LENGTH_MAX("bodyLengthMax"),
    TOKEN_COUNT_MAX("tokenCountMax"),
    PATTERN_LEVEL_MAX("patternLevelMax");

    private String typeString;

    PatternMetaType(String typeString) {
        this.typeString = typeString;
    }

    public String getTypeString() {
        return typeString;
    }

    public static PatternMetaType fromString(String text)
            throws IllegalArgumentException {
        for (PatternMetaType type : PatternMetaType.values()) {
            if (type.typeString.equalsIgnoreCase(text)) {
                return type;
            }
        }
        throw new IllegalArgumentException("No meta type with text " + text + " found");
    }

}
