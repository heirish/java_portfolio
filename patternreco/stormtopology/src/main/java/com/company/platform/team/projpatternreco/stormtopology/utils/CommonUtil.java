package com.company.platform.team.projpatternreco.stormtopology.utils;

import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Created by Administrator on 2018/8/5 0005.
 */
public class CommonUtil {

    public static double round(double value, int precision) {
        BigDecimal bd = new BigDecimal(value)
                .setScale(precision, RoundingMode.HALF_EVEN);
        return bd.doubleValue();
    }

    public static boolean equalWithPrecision(double value1, double value2, int precision) {
        double precisionFactor = Math.pow(10, precision);
        BigDecimal bdValue1 = new BigDecimal(value1 * precisionFactor).setScale(0, RoundingMode.HALF_EVEN);
        BigDecimal bdValue2 = new BigDecimal(value2 * precisionFactor).setScale(0, RoundingMode.HALF_EVEN);
        return StringUtils.equals(bdValue1.toString(), bdValue2.toString());
    }
}
