package com.company.platform.team.projpatternreco.stormtopology.refinder;

import com.company.platform.team.projpatternreco.common.utils.ListUtil;
import com.company.platform.team.projpatternreco.modules.NeedlemanWunschAligner;

import java.util.List;

/**
 * Created by admin on 2018/7/18.
 */
public class Aligner {
    private static NeedlemanWunschAligner aligner = new NeedlemanWunschAligner(1, -1, -2);

    public static List<String> retrievePattern(List<String> sequence1, List<String> sequence2) throws Exception {
        List<String> alignedSequence = aligner.traceBack(sequence1, sequence2,
                aligner.createScoreMatrix(sequence1, sequence2));
        return ListUtil.removeExcessiveDuplicates(alignedSequence, "*", 5);
    }
}
