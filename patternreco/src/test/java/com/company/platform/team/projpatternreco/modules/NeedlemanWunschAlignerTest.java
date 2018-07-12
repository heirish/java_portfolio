package com.company.platform.team.projpatternreco.modules;

import com.company.platform.team.projpatternreco.common.preprocess.Tokenizer;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

/**
 * Created by admin on 2018/6/21.
 */
public class NeedlemanWunschAlignerTest {
    private static final Logger logger = Logger.getLogger("");
    static NeedlemanWunschAligner aligner;
    static List<String> tokens1 = Tokenizer.simpleTokenize("this is a test string for alignment");
    static List<String> tokens2 = Tokenizer.simpleTokenize("how about write a test case for alignment");

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void initialize() {
        aligner = new NeedlemanWunschAligner(1, -1, -2);
    }

    @Test
    public void createScoreMatrixTest() {
        // int[][] rightResult = new int[][]{
        //         [[0, -2, -4, -6, -8, -10, -12, -14, -16, -18, -20, -22, -24, -26, -28, -30], [-2, -1, -3, -5, -7, -9, -11, -13, -15, -17, -19, -21, -23, -25, -27, -29], [-4, -3, 0, -2, -4, -6, -8, -10, -12, -14, -16, -18, -20, -22, -24, -26], [-6, -5, -2, -1, -3, -5, -7, -9, -11, -13, -15, -17, -19, -21, -23, -25], [-8, -7, -4, -3, 0, -2, -4, -6, -8, -10, -12, -14, -16, -18, -20, -22], [-10, -9, -6, -5, -2, -1, -3, -3, -5, -7, -9, -11, -13, -15, -17, -19], [-12, -11, -8, -7, -4, -3, 0, -2, -2, -4, -6, -8, -10, -12, -14, -16], [-14, -13, -10, -9, -6, -5, -2, -1, -3, -1, -3, -5, -7, -9, -11, -13], [-16, -15, -12, -11, -8, -7, -4, -3, 0, -2, 0, -2, -4, -6, -8, -10], [-18, -17, -14, -13, -10, -9, -6, -5, -2, -1, -2, -1, -3, -5, -7, -9], [-20, -19, -16, -15, -12, -11, -8, -7, -4, -3, 0, -2, 0, -2, -4, -6], [-22, -21, -18, -17, -14, -13, -10, -9, -6, -5, -2, -1, -2, 1, -1, -3], [-24, -23, -20, -19, -16, -15, -12, -11, -8, -7, -4, -3, 0, -1, 2, 0], [-26, -25, -22, -21, -18, -17, -14, -13, -10, -9, -6, -5, -2, -1, 0, 3]]
        // };
        try {
            int[][] matrix = aligner.createScoreMatrix(tokens1, tokens2);
            logger.info(Arrays.deepToString(matrix));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void traceBackTest(){
        try {
            int[][] matrix = aligner.createScoreMatrix(tokens1, tokens2);
            List<String> alignedTokens = aligner.traceBack(tokens1, tokens2, matrix);
            System.out.println(alignedTokens.toString());
            System.out.println(String.join("", alignedTokens));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
