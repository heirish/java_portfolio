package com.company.platform.team.projpatternreco.common.preprocess;

import com.company.platform.team.projpatternreco.common.data.MatchedSlice;
import org.apache.commons.lang.StringUtils;
import org.apache.http.conn.util.InetAddressUtils;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by admin on 2018/6/20.
 */
public class Identifier {
    private static final Logger logger = Logger.getLogger("");

    private static String IPV4_SEG  = "(25[0-5]|(2[0-4]|1?[0-9])?[0-9])";
    private static String IPV4_ADDR = "(" + IPV4_SEG + "\\.){3}" + IPV4_SEG;
    private static String IPV6_SEG  = "[0-9a-fA-F]{1,4}";
    private static String IPV6_ADDR = "((" + IPV6_SEG + ":){7}" + IPV6_SEG + "|"
            + IPV6_SEG + ":((:" + IPV6_SEG + "){1,6})|"
            + "[fF][eE]80:(:" + IPV6_SEG + "){0,4}%[0-9a-zA-Z]+|"
            + "[fF][eE]80:(:" + IPV6_SEG + "){0,4}|"
            + "::(ffff(:0{1,4})?:)?" + IPV4_ADDR + "|"
            + "(" + IPV6_SEG + ":){1,4}:" + IPV4_ADDR + ")";
    private static Pattern IPV4_PATTERN =  Pattern.compile(IPV4_ADDR,
            Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL|Pattern.UNICODE_CASE);
    private static Pattern IPV6_PATTERN = Pattern.compile(IPV6_ADDR,
            Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL|Pattern.UNICODE_CASE);

    private static DateFinderRegex dateFinderRegex = new DateFinderRegex();

    public static final String identifyIP(String text, String typeName){
       return identifyIPV6(identifyIPV4(text, typeName), typeName);
    }

    public static String identifyIPV4(String text, String typeName) {
        Matcher matcher = IPV4_PATTERN.matcher(text);
        int lastIndex = 0;
        String retText = "";
        while (matcher.find()) {
            String matchedIP = matcher.group();
            if (InetAddressUtils.isIPv4Address(matchedIP)) {
                retText += text.substring(lastIndex, matcher.start()).concat(typeName);
                lastIndex = matcher.end();
            }
            logger.debug(String.format("matched IP: %s", matchedIP));
        }

        if (lastIndex  > 0) {
            retText += text.substring(lastIndex);
        }

        return (retText.length() > 0 ? retText : text);
    }

    public static String identifyIPV6(String text, String typeName) {
        Matcher matcher = IPV6_PATTERN.matcher(text);
        int lastIndex = 0;
        StringBuilder stringBuilder = new StringBuilder();
        while (matcher.find()) {
            String matchedIP = matcher.group();
            if (InetAddressUtils.isIPv6Address(matchedIP)) {
                stringBuilder.append(text.substring(lastIndex, matcher.start()));
                stringBuilder.append(typeName);
                lastIndex = matcher.end();
            }
            logger.debug(String.format("matched IP: %s", matchedIP));
        }

        if (lastIndex  > 0) {
            stringBuilder.append(text.substring(lastIndex));
        }

        return (stringBuilder.length() > 0 ? stringBuilder.toString(): text);
    }

    public static String identifyDatetime(String text, String typeName) {
        List<MatchedSlice> slices = findDates(text, "natty");
        if (slices == null || slices.size() == 0) {
            return text;
        }

        StringBuilder stringBuilder = new StringBuilder();
        int lastIndex = 0;
        for (MatchedSlice slice : slices) {
            stringBuilder.append(text.substring(lastIndex, slice.startIndex));
            stringBuilder.append(typeName);
            lastIndex = slice.endIndex;
        }

        if (lastIndex > 0) {
            stringBuilder.append(text.substring(lastIndex));
        }
        return (stringBuilder.length() > 0 ? stringBuilder.toString() : text);
    }

    private static List<MatchedSlice> findDates(String text, String finderName) {
        if (StringUtils.equalsIgnoreCase(finderName, "natty")) {
            return DateFinderNatty.findDates(text);
        } else {
            DateFinderRegex regexFinder = new DateFinderRegex();
            return regexFinder.findDateTimes(text);
        }
    }
}
