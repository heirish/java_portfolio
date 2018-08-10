package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.company.platform.team.projpatternreco.common.utils.ListUtil;
import com.company.platform.team.projpatternreco.common.modules.NeedlemanWunschAligner;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by admin on 2018/7/18.
 */
public class Aligner {
    private static Logger logger = LoggerFactory.getLogger(Aligner.class);
    private static NeedlemanWunschAligner aligner = new NeedlemanWunschAligner(1, -1, -2);

    public static List<String> retrievePattern(List<String> tokensLeft, List<String> tokensRight) throws Exception {
        List<String> alignedSequence = aligner.traceBack(tokensLeft, tokensRight,
                aligner.createScoreMatrix(tokensLeft, tokensRight));
        if (onlyWildcatsOrEmpty(alignedSequence)) {
            logger.info("tokensLeft: " + tokensLeft);
            logger.info("tokensRight: " + tokensRight);
            logger.info("alignedTokens: " + alignedSequence);
        }
        return ListUtil.removeExcessiveDuplicates(alignedSequence, "*", 5);
    }

    // only for test
    public static boolean onlyWildcatsOrEmpty(List<String> alignedSequence) {
        boolean result = true;
        if (alignedSequence != null && alignedSequence.size() > 0) {
            String alignedString = String.join("", alignedSequence);
            alignedString = alignedString.replace("*", "");
            if (!StringUtils.isBlank(alignedString)) {
                result = false;
            }
        }
        return result;
    }
}
