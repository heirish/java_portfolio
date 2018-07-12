package com.company.platform.team.projpatternreco.common.preprocess;

import org.junit.Test;

/**
 * Created by admin on 2018/6/26.
 */
public class DateFinderRegexTest {
    @Test
    public void findDateTimesTest1() {
        //String text = "2018-06-22T16:48:22.099+0900 I -        [conn16] Assertion: 10334:";
        String text = "2018-06-22T16:48:22.099 I -        [conn16] Assertion: 10334:";
        DateFinderNatty.findDates(text);
    }

    @Test
    public void findDateTimesTest2() {
        String text = "\n)(2018-06-25 05:44:45 +0000)";
        DateFinderNatty.findDates(text);
    }

    @Test
    public void findDateTimesTest3() {
        String text = "\n)(2018-06-25 05am)";
        DateFinderNatty.findDates(text);
    }
}
