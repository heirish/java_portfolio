package com.company.platform.team.projspark.common.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

/**
 * Created by admin on 2018/6/26.
 */
public class DateTimeUtil {
    private static final String DATE_FORMAT = "yyyy MM dd";
    private static final String DATE_FORMAT_WITH_DAYNAME = String.format("E %s", DATE_FORMAT);

    private static final String TIME_FORMAT = "HH mm SS z";

    private static final String DATETIME_FORMAT = String.format("%s %s", DATE_FORMAT, TIME_FORMAT);
    private static final String DATETIME_FORMAT_WITH_DAYNAME = String.format("E %s", DATETIME_FORMAT);

    private static Pattern DELIMITERS_PATTERN = Pattern.compile("[/:\\-,.\\s_+]+");

    public static LocalDateTime tryParse(String datetimeString) throws Exception {
        String strippedString = DELIMITERS_PATTERN.matcher(datetimeString).replaceAll(" ");
        return LocalDateTime.parse(strippedString,
                DateTimeFormatter.ofPattern(DATETIME_FORMAT));
    }

//    public static LocalDateTime tryParse(String datetimeString) throws Exception {
//        DateTime dateTime = new DateTime();
//
//        String splitedText = Pattern.compile("(\\s+)")
//                .matcher(datetimeString).replaceAll(" $1 ");
//        splitedText = DELIMITERS_PATTERN.matcher(splitedText).replaceAll(" $1 ");
//        List<String> tokens = Arrays.asList(splitedText.split("\\s"));
//        for (String token : tokens) {
//            if (StringUtils.isNotEmpty(token)){
//                if (StringUtils.isNumeric(token)) {
//                   long digit = Long.valueOf(token);
//                   if (digit > 9999 && token.length() == 6) {
//                       dateTime.year = digit / 10000;
//                       dateTime.month = (digit % 10000) / 100;
//                       dateTime.day = (digit % 100) / 10 ;
//                   }
//                   if (digit >= 1970 && digit < 10000) {
//                       dateTime.year = digit;
//                   } //else if (digit )
//                }
//                }
//        }
//
//        return null;
//    }
};
