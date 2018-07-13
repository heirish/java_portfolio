package com.company.platform.team.projpatternreco;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.junit.Test;

import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;

/**
 * Created by admin on 2018/7/13.
 */
public class UsageTest {
    private static final Gson gson = new Gson();

    @Test
    public void readResourceFile() {
        try {
            String fileName = "PatternRecognize.json";
            InputStream file = Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(fileName));
            //Type mapType = new TypeToken<Map<String, String>>() {}.getType();
            Map<String, String> jsonMap = gson.fromJson(new InputStreamReader(file), Map.class);
            //String file = Objects.requireNonNull(this.getClass().getClassLoader().getResource(fileName)).getFile();
            //Map<String, String> jsonMap = gson.fromJson(new JsonReader(new FileReader(file)), Map.class);
            System.out.println(jsonMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
