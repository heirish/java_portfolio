package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.google.gson.Gson;

/**
 * Created by Administrator on 2018/8/5 0005.
 */
public class GsonFactory {
    private static final Gson gson = new Gson();

    public static Gson getGson() {
        return gson;
    }
}
