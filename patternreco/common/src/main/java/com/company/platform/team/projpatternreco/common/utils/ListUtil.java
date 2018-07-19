package com.company.platform.team.projpatternreco.common.utils;

import org.apache.commons.lang3.StringUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by admin on 2018/6/22.
 */
public class ListUtil {
    //TODO: generic programming
    public static List<String> removeExcessiveDuplicates(List<String> tokens, String duplicate, int maxDuplicate) {
        int count = 0;
        List<String> retTokens = new ArrayList<>(tokens);
        Iterator iterator = retTokens.iterator();
        while (iterator.hasNext()) {
            String token = (String) iterator.next();
            if (StringUtils.equals(token, duplicate) || StringUtils.isEmpty(token)) {
                count ++;
            } else {
                count = 0;
            }

            if (count > maxDuplicate) {
                iterator.remove();
            }
        }
        return retTokens;
    }
}
