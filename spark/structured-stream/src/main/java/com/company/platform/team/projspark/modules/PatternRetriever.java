package com.company.platform.team.projspark.modules;

import com.company.platform.team.projspark.data.Constants;
import com.company.platform.team.projspark.data.PatternLevelTree;
import com.company.platform.team.projspark.data.PatternNode;
import com.company.platform.team.projspark.utils.ListUtil;
import com.google.gson.Gson;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/6/21.
 */
public class PatternRetriever {

    private static NeedlemanWunschAligner aligner = new NeedlemanWunschAligner(1, -1, -2);
    private static final Gson gson = new Gson();
    private static final Logger logger = Logger.getLogger("");;

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
            extends Reducer<Text, Text, NullWritable, Text> {
        //private Map<String, PatternNode> parentNodes;
        private int thisLevel;
        private double maxDist;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            thisLevel = Integer.parseInt(conf.get("level"));
            maxDist = Double.parseDouble(conf.get("level.maxDist"));
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            try {
                String[] keyItems = key.toString().split(Constants.PATTERN_NODE_KEY_DELIMITER);
                String projectName = keyItems[0];
                int nodeLevel = Integer.parseInt(keyItems[1]);
                String nodeId = keyItems[2];

                PatternNode node = PatternLevelTree.getInstance().getNode(key.toString());
                List<String> newTokens = node.getPatternTokens();
                for (Text value : values) {
                    List<String> tokens2 = Arrays.asList(value.toString().split(Constants.PATTERN_NODE_KEY_DELIMITER));
                    newTokens = retrievePattern(newTokens, tokens2);
                }
                node.updatePatternTokens(newTokens);

                if (!node.hasParent()) {
                    String parentId = PatternLevelTree.getInstance()
                            .getParentNodeId(newTokens, projectName, nodeLevel+1, maxDist);
                    node.setParent(parentId);
                }

                //update the tree node(parent Id and pattern) by key
                PatternLevelTree.getInstance().updateNode(projectName, nodeLevel, nodeId, node);

                Text reduceOutValue = new Text();
                Map<String, String> jsonItems = new HashMap<>();
                jsonItems.put(Constants.FIELD_PATTERNID, key.toString());
                jsonItems.put(Constants.FIELD_REPRESENTTOKENS,
                        String.join(Constants.PATTERN_NODE_KEY_DELIMITER, node.getRepresentTokens()));
                jsonItems.put(Constants.FIELD_PATTERNTOKENS,
                        String.join(Constants.PATTERN_NODE_KEY_DELIMITER, node.getPatternTokens()));
                reduceOutValue.set(gson.toJson(jsonItems));
                context.write(NullWritable.get(), reduceOutValue);
            } catch (Exception e) {
                logger.error("key: "+key.toString(), e);
            }
        }
    }

   public static List<String> retrievePattern(List<String> sequence1, List<String> sequence2) throws Exception {
        List<String> alignedSequence = aligner.traceBack(sequence1, sequence2,
                aligner.createScoreMatrix(sequence1, sequence2));
        return ListUtil.removeExcessiveDuplicates(alignedSequence, "*", 5);
    }
}
