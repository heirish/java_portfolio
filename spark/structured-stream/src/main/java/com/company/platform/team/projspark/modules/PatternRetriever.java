package com.company.platform.team.projspark.modules;

import com.company.platform.team.projspark.data.Constants;
import com.company.platform.team.projspark.data.PatternForest;
import com.company.platform.team.projspark.preprocess.Tokenizer;
import com.company.platform.team.projspark.utils.ListUtil;
import com.google.gson.Gson;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/6/21.
 */
public class PatternRetriever {

    private static NeedlemanWunschAligner aligner = new NeedlemanWunschAligner(1, -1, -2);
    private static final Gson gson = new Gson();

    // https://stackoverflow.com/questions/11784729/hadoop-java-lang-classcastexception-org-apache-hadoop-io-longwritable-cannot
    public static class ParentNodeMapper
            //extends Mapper<Text, Text, Text, Text> {
            extends Mapper<Object, Text, Text, Text> {

        //public void map(Text key, Text value, Context context)
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Map<String, String> fields = gson.fromJson(value.toString(), Map.class);
            if (fields != null && fields.containsKey(Constants.FIELD_PATTERNID)
                    && fields.containsKey(Constants.FIELD_PATTERNTOKENS)) {
                Text mapoutKey = new Text();
                Text mapoutValue = new Text();
                mapoutKey.set(fields.get(Constants.FIELD_PATTERNID));
                mapoutValue.set(fields.get(Constants.FIELD_PATTERNTOKENS));
                context.write(mapoutKey, mapoutValue);
            }
        }
    }

    public static class PatternRetrieveReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            try {
            List<String> tokens1 = PatternForest.getInstance().getNode(key.toString()).getPatternTokens();
            for (Text value : values) {
                List<String> tokens2 = Tokenizer.simpleTokenize(value.toString());
                tokens1 = retrievePattern(tokens1, tokens2);
            }
            //TODO:update the tree node by key
            Text reduceOutValue = new Text();
            reduceOutValue.set(String.join("", tokens1));
            } catch (Exception e) {
               e.printStackTrace();
            }
        }
    }

   public static List<String> retrievePattern(List<String> sequence1, List<String> sequence2) throws Exception {
        List<String> alignedSequence = aligner.traceBack(sequence1, sequence2,
                aligner.createScoreMatrix(sequence1, sequence2));
        return ListUtil.removeExcessiveDuplicates(alignedSequence, "*", 5);
    }
}
