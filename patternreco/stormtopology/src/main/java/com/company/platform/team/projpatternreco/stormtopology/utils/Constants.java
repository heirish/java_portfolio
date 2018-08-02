package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;

/**
 * Created by admin on 2018/6/22.
 */
public class Constants {
    //log fields
    public static final String FIELD_PROJECTNAME = "projectName";
    public static final String FIELD_BODY = "body";
    public static final String FIELD_LEAFID = "leafId";
    public static final String FIELD_PATTERNID = "@patternId";
    public static final String FIELD_REPRESENTTOKENS = "@representTokens";
    public static final String FIELD_PATTERNTOKENS = "@patternTokens";
    public static final String FIELD_META_TYPE = "@metaType";
    public static final String FIELD_META_KEY = "@metaKey";

    //patternreco
    public static final String CONFIGURE_PATTERNRECO_SECTION = "patternreco";
    public static final String CONFIGURE_REDIS_SECTION = "patternreco";
    public static final double PATTERN_LEAF_SIMILARITY_DEFAULT = 0.9;
    public static final double PATTERN_LEAF_SIMILARITY_MIN = 0.1;
    public static final double PATTERN_LEAF_SIMILARITY_MAX = 0.9;
    public static final double SIMILARITY_DECAY_FACTOR_DEFAULT = 0.1;
    public static final int PATTERN_LEAF_COUNT_MAX_DEFAULT = 500;
    public static final int FINDCLUSTER_TOLERANCE_TIMES = 4;
    public static final int BODY_LENGTH_MAX_DEFAULT = 5000;
    public static final int TOKEN_COUNT_MAX_DEFAULT = 200;
    public static final long LEAF_PATTERN_CACHE_SECONDS_DEFAULT = 60;
    public static final long LEAF_PATTERN_CACHE_MAX_DEFAULT = 500;
    public static final int PATTERN_LEVEL_MAX_DEFAULT = 10;
    public static final String IDENTIFY_EXCEEDED_TYPE = "NELO_ELIMINATED";
    public static final String PATTERN_TOKENS_DELIMITER = "%@%";
    public static final String FIELD_DELIMITER_DEFAULT = "¬";

    //Storm
    public static final String LOG_OUT_STREAMID = "log";
    public static final String PATTERN_UNMERGED_STREAMID = "unmerged";
    public static final String PATTERN_UNADDED_STREAMID = "unadded";
    public static final String PATTERN_META_STREAMID = "meta";

    public static final Type LOG_MAP_TYPE = new TypeToken<HashMap<String, String>>() {}.getType();
}
