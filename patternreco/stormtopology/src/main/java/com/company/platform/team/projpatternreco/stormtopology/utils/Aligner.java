package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.company.platform.team.projpatternreco.common.utils.ListUtil;
import com.company.platform.team.projpatternreco.common.modules.NeedlemanWunschAligner;

import java.util.List;

/**
 * Created by admin on 2018/7/18.
 */
public class Aligner {
    private static NeedlemanWunschAligner aligner = new NeedlemanWunschAligner(1, -1, -2);

    public static List<String> retrievePattern(List<String> tokensLeft, List<String> tokensRight) throws Exception {
        List<String> alignedSequence = aligner.traceBack(tokensLeft, tokensRight,
                aligner.createScoreMatrix(tokensLeft, tokensRight));
        return ListUtil.removeExcessiveDuplicates(alignedSequence, "*", 5);
    }
}
