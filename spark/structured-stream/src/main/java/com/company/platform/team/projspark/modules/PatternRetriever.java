package com.company.platform.team.projspark.modules;

import com.company.platform.team.projspark.utils.ListUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * Created by admin on 2018/6/21.
 */
public class PatternRetriever {

    private static NeedlemanWunschAligner aligner = new NeedlemanWunschAligner(1, -1, -2);

    public static class ParentNodeMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

        }
    }

    public static class PatternRetrieveReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {

        }

    }


   public static List<String> retrievePattern(List<String> sequence1, List<String> sequence2) throws Exception {
        List<String> alignedSequence = aligner.traceBack(sequence1, sequence2,
                aligner.createScoreMatrix(sequence1, sequence2));
        return ListUtil.removeExcessiveDuplicates(alignedSequence, "*", 5);
    }
}
