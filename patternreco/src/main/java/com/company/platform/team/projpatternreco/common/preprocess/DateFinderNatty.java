package com.company.platform.team.projpatternreco.common.preprocess;

import com.company.platform.team.projpatternreco.common.data.MatchedSlice;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by admin on 2018/6/26.
 */
public class DateFinderNatty {
    private static Parser parser  = new Parser();

    public static List<MatchedSlice> findDates(String text) {
        List<MatchedSlice> slices = new ArrayList<>();
        List<DateGroup> groups = parser.parse(text);
        for (DateGroup group : groups) {
            List<Date> dates = group.getDates();
            MatchedSlice slice = new MatchedSlice();
            slice.startIndex = group.getAbsolutePosition();
            slice.matchedString = group.getText();
            slice.endIndex = slice.startIndex + slice.matchedString.length();
            slices.add(slice);
        }
        return slices;
    }
}
