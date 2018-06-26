package com.company.platform.team.projspark.preprocess;

import com.company.platform.team.projspark.data.MatchedSlice;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

import java.util.Date;
import java.util.List;

/**
 * Created by admin on 2018/6/26.
 */
public class DateFinderNatty {
    private static Parser parser  = new Parser();

    public static List<MatchedSlice> findDates(String text) {
        List<DateGroup> groups = parser.parse(text);
        for (DateGroup group : groups) {
            List<Date> dates = group.getDates();
            int line = group.getLine();
            int column = group.getPosition();
            String matchingValue = group.getText();
            String syntaxTree = group.getSyntaxTree().toStringTree();
            boolean isRecurreing = group.isRecurring();
            System.out.println(dates.toString());
            System.out.println(matchingValue);
        }
        return null;
    }
}
