package com.company.platform.team.projpatternreco.common.preprocess;

import java.util.List;

/**
 * Created by admin on 2018/6/29.
 */
public class Preprocessor {

    //TODO: pipline
    public static List<String> transform(String text) {
        String preprocessedText = Identifier.identifyIP(text, "NELO_IP");
        //preprocessedText = Identifier.identifyDatetime(preprocessedText, "NELO_DATETIME");

        return Tokenizer.simpleTokenize(preprocessedText);
    }
}
