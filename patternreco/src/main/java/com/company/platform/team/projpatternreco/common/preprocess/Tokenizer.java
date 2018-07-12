package com.company.platform.team.projpatternreco.common.preprocess;

import com.company.platform.team.projpatternreco.common.utils.ListUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by admin on 2018/6/20.
 */
public class Tokenizer {
    private static final Logger logger = Logger.getLogger("");
    private static String PUNCTUATIONS_EN = "!\"//$%&'()+,./:;<=>?@\\[\\]\\\\^`{|}~";
    private static String NONSTOPS_CN =
            // Fullwidth ASCII variants
            "\uFF02\uFF03\uFF04\uFF05\uFF06\uFF07\uFF08\uFF09\uFF0A\uFF0B\uFF0C\uFF0D"
            + "\uFF0F\uFF1A\uFF1B\uFF1C\uFF1D\uFF1E\uFF20\uFF3B\uFF3C\uFF3D\uFF3E\uFF3F"
            + "\uFF40\uFF5B\uFF5C\uFF5D\uFF5E\uFF5F\uFF60"
            // Halfwidth CJK punctuation
            + "\uFF62\uFF63\uFF64"
            // CJK symbols and punctuation
            + "\u3000\u3001\u3003"
            // CJK angle and corner brackets
            + "\u3008\u3009\u300A\u300B\u300C\u300D\u300E\u300F\u3010\u3011"
            // CJK brackets and symbols/punctuation
            + "\u3014\u3015\u3016\u3017\u3018\u3019\u301A\u301B\u301C\u301D\u301E\u301F"
            // Other CJK symbols
            + "\u3030"
            // Special CJK indicators
            + "\u303E\u303F"
            // Dashes
            + "\u2013\u2014"
            // Quotation marks and apostrophe
            + "\u2018\u2019\u201B\u201C\u201D\u201E\u201F"
            // General punctuation
            + "\u2026\u2027"
            // Overscores and underscores
            + "\uFE4F"
            // Small form variants
            + "\uFE51\uFE54"
            // Latin punctuation
            + "\u00B7";
    private static String STOPS_CN =
            "\uFF01"  // Fullwidth exclamation mark
            + "\uFF1F"  // Fullwidth question mark
            + "\uFF61"  // Halfwidth ideographic full stop
            + "\u3002";  // Ideographic full stop

    private static String PUNCTUATIONS = PUNCTUATIONS_EN + NONSTOPS_CN + STOPS_CN;
    private static Pattern PUNCTUATIONS_PATTERN = Pattern.compile(String.format("([%s]+)", PUNCTUATIONS),
            Pattern.MULTILINE|Pattern.DOTALL|Pattern.UNICODE_CASE);

    public static List<String> simpleTokenize(String text) {
        String splitedText = Pattern.compile("(\\s+)", Pattern.MULTILINE | Pattern.DOTALL)
                .matcher(text).replaceAll(" $1 ");
        splitedText = PUNCTUATIONS_PATTERN.matcher(splitedText).replaceAll(" $1 ");

        logger.debug("Splited text: " + splitedText);
        List<String> tokens = Arrays.asList(splitedText.split("\\s"));
        logger.debug("tokens: " + tokens.toString());
        for (int i=0; i<tokens.size(); i++) {
           if (StringUtils.isNotEmpty(tokens.get(i))){
               if (StringUtils.isNumeric(tokens.get(i))) {
                   tokens.set(i, "NELO_NUM");
                }
           } else {
               tokens.set(i, " ");
           }
        }
        tokens = ListUtil.removeExcessiveDuplicates(tokens, " ", 1);
        logger.debug("after remove duplicates tokens: " + tokens.toString());
        return tokens;
    }
}
