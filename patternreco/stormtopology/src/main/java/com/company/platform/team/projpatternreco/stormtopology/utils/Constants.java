package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;

/**
 * Created by admin on 2018/6/22.
 */
public class Constants {
    public static final String NEW_LINE_SEPARATOR = "\n";
    public static final int FINDCLUSTER_TOLERANCE_TIMES = 4;
    public static final String FIELD_BODY = "body";
    public static final String FIELD_PROJECTNAME = "projectName";
    public static final String FIELD_LEAFID = "leafId";
    public static final String FIELD_PATTERNID = "@patternId";
    public static final String FIELD_REPRESENTTOKENS= "@representTokens";
    public static final String FIELD_PATTERNTOKENS= "@patternTokens";
    public static final String PATTERN_DIST_DECAY_TYPE = "exp";
    public static final String PATTERN_TOKENS_DELIMITER = "%@%";

    public static final int MAX_PATTERN_LEVEL = 10;
    public static final double PATTERN_LEAF_SIMILARITY_DEFAULT = 0.9;
    public static final double PATTERN_SIMILARITY_STEPWISE = 0.1;
    public static final String IDENTIFY_EXCEEDED_TYPE = "NELO_ELIMINATED";

    //Storm
    public static final String LOG_OUT_STREAMID = "log";
    public static final String PATTERN_UNMERGED_STREAMID = "unmerged";
    public static final String PATTERN_UNADDED_STREAMID = "unadded";

    public static final Type LOG_MAP_TYPE = new TypeToken<HashMap<String, String>>() {}.getType();
}
