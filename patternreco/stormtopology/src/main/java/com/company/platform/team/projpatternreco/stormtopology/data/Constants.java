package com.company.platform.team.projpatternreco.stormtopology.data;

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
    public static final String FIELD_PATTERNTOKENS = "@patternTokens";
    public static final String FIELD_META_TYPE = "@metaType";

    //patternreco
    public static final String CONFIGURE_PATTERNRECO_SECTION = "patternreco";
    public static final String CONFIGURE_REDIS_SECTION = "redis";
    public static final String CONFIGURE_MYSQL_SECTION = "mysql";
    public static final long LEAF_PATTERN_CACHE_SECONDS_DEFAULT = 10;
    public static final long LEAF_PATTERN_CACHE_MAX_DEFAULT = 200;
    public static final String IDENTIFY_EXCEEDED_TYPE = "NELO_ELIMINATED";
    public static final String PATTERN_TOKENS_DELIMITER = "%@%";

    //Storm
    public static final String LOG_OUT_STREAMID = "log";
    public static final String PATTERN_UNMERGED_STREAMID = "unmerged";
    public static final String PATTERN_UNADDED_STREAMID = "unadded";
    public static final String PATTERN_META_STREAMID = "meta";
    public static final String REDIS_FLUSH_STREAMID = "redisFlush";
    public static final int SIMILARITY_PRECISION = 2;

    public static final Type LOG_MAP_TYPE = new TypeToken<HashMap<String, String>>() {}.getType();
}
