package com.company.platform.team.projspark.modules;

import com.company.platform.team.projspark.preprocess.Tokenizer;
import com.company.platform.team.projspark.utils.ListUtil;

import java.util.List;

/**
 * Created by admin on 2018/6/21.
 */
public class PatternRetriever {

    private static NeedlemanWunschAligner aligner = new NeedlemanWunschAligner(1, -1, -2);

    public static List<String> retrievePattern(List<String> sequence1, List<String> sequence2) throws Exception {
        List<String> alignedSequence = aligner.traceBack(sequence1, sequence2,
                aligner.createScoreMatrix(sequence1, sequence2));
        return ListUtil.removeExcessiveDuplicates(alignedSequence, "*", 5);
    }
}
