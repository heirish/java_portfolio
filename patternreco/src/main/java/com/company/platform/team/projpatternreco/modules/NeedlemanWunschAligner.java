package com.company.platform.team.projpatternreco.modules;

import org.apache.commons.lang.StringUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by admin on 2018/6/21.
 */
public class NeedlemanWunschAligner {
    private int equalScore;
    private int unequalScore;
    private int spaceScore;

    public NeedlemanWunschAligner(int equalScore, int unequalScore, int spaceScore) {
        this.equalScore = equalScore;
        this.unequalScore = unequalScore;
        this.spaceScore = spaceScore;
    }

    //TODO:generic programming
    public int[][] createScoreMatrix (List<String> tokens1, List<String> tokens2) throws Exception{
        int rows = tokens1.size();
        int columns = tokens2.size();
        System.out.println(String.format("rows %s, columns %s", rows, columns));
        if (rows == 0 || columns == 0) {
            throw new Exception(String.format("One of the tokens lists size if 0, tokens1 size: %s, tokens2 size: %s",
                    rows, columns));
        }
        int[][] matrix = new int[rows+1][columns+1];
        for (int[] row: matrix) {
            Arrays.fill(row, 0);
        }
        for (int i=1; i<rows+1; i++) {
            matrix[i][0] = i * spaceScore;
        }
        for (int j=1; j<columns+1; j++) {
            matrix[0][j] = j * spaceScore;
        }

        for (int i=0; i<rows; i++) {
            for (int j=0; j<columns;j++) {
                if (StringUtils.equalsIgnoreCase(tokens1.get(i), tokens2.get(j))) {
                    matrix[i+1][j+1] = matrix[i][j] + equalScore;
                } else {
                    matrix[i+1][j+1] = Math.max(matrix[i][j+1] + spaceScore,
                            Math.max(matrix[i+1][j]+spaceScore, matrix[i][j] + unequalScore));
                }
            }
        }

        return matrix;
    }

    public List<String> traceBack(List<String> sequence1, List<String> sequence2, int[][] scoreMatrix) throws Exception{
        int matrixRows = scoreMatrix.length;
        int matrixColumns = scoreMatrix[0].length;
        if (matrixRows<=1 || matrixColumns<=1) {
            throw new Exception(String.format("Score matrix is illegal, rows: %s, columns: %s",
                    matrixRows, matrixColumns));
        }

        //List<String> commSubsequence = new ArrayList<>();
        //List<String> alignedSequence1 =  new ArrayList<>();
        //List<String> alignedSequence2 =  new ArrayList<>();
        List<String> alignedSequence =  new ArrayList<>();
        int i = matrixRows - 1;
        int j = matrixColumns - 1;
        while (i>0 && j>0) {
            if (StringUtils.equalsIgnoreCase(sequence1.get(i-1), sequence2.get(j-1))) {
                // commSubsequence.add(sequence1.get(i-1));
                // alignedSequence1.add(sequence1.get(i-1));
                // alignedSequence2.add(sequence2.get(j-1));
                alignedSequence.add(sequence1.get(i-1));
                i --;
                j --;
            } else if (scoreMatrix[i][j] == scoreMatrix[i-1][j-1] + unequalScore) {
                // alignedSequence1.add(sequence1.get(i-1));
                // alignedSequence2.add(sequence2.get(j-1));
                alignedSequence.add("*");
                i --;
                j --;
            } else if (scoreMatrix[i][j] == scoreMatrix[i-1][j] + spaceScore) {
                // alignedSequence1.add(sequence1.get(i-1));
                // alignedSequence2.add(" ");
                alignedSequence.add("*");
                i --;
            } else {
                alignedSequence.add("*");
                // alignedSequence1.add(" ");
                // alignedSequence2.add(sequence2.get(j-1));
                j --;
            }
        }

        if (i>0) {
            // alignedSequence1.addAll(i, sequence1);
            String[] wildcats = new String[i];
            Arrays.fill(wildcats, "*");
            alignedSequence.addAll(Arrays.asList(wildcats));
        } else if (j>0){
            // alignedSequence1.addAll(i, sequence1);
            String[] wildcats = new String[j];
            Arrays.fill(wildcats, "*");
            alignedSequence.addAll(Arrays.asList(wildcats));
        }

        // Collections.reverse(commSubsequence);
        // Collections.reverse(alignedSequence1);
        // Collections.reverse(alignedSequence2);
        Collections.reverse(alignedSequence);
        //logger.debug("CommSubsequence: " + commSubsequence);
        //logger.debug("AlignedSequence1: " + alignedSequence1);
        //logger.debug("AlignedSequence2: " + alignedSequence2);

        return alignedSequence;
    }
}
