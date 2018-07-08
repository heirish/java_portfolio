package com.company.platform.team.projspark.PatternRefiner;

import com.company.platform.team.projspark.common.data.Constants;
import com.company.platform.team.projspark.common.data.PatternNode;
import com.company.platform.team.projspark.common.data.PatternNodeKey;
import com.company.platform.team.projspark.common.utils.ListUtil;
import com.company.platform.team.projspark.modules.NeedlemanWunschAligner;
import com.google.gson.Gson;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/6/21.
 */
public class MapReduceRefinerWorker {

    private static NeedlemanWunschAligner aligner = new NeedlemanWunschAligner(1, -1, -2);
    private static final Gson gson = new Gson();

    // https://stackoverflow.com/questions/11784729/hadoop-java-lang-classcastexception-org-apache-hadoop-io-longwritable-cannot
    // input current tokens and parentid
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
                String parentId = fields.get(Constants.FIELD_PATTERNID);
                if (!StringUtils.isEmpty(parentId)) {
                    mapoutKey.set(parentId);
                    mapoutValue.set(fields.get(Constants.FIELD_PATTERNTOKENS));
                    context.write(mapoutKey, mapoutValue);
                }
            }
        }
    }

    //output parent token and parent's parentid
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
                PatternNodeKey nodeKey = PatternNodeKey.fromString(key.toString());

                PatternNode node = PatternLevelTree.getInstance().getNode(nodeKey);
                List<String> newTokens = node.getPatternTokens();
                for (Text value : values) {
                    List<String> tokens2 = Arrays.asList(value.toString().split(Constants.PATTERN_NODE_KEY_DELIMITER));
                    newTokens = retrievePattern(newTokens, tokens2);
                }
                node.updatePatternTokens(newTokens);

                PatternNodeKey parentNodeKey = null;
                if (!node.hasParent()) {
                    parentNodeKey = PatternLevelTree.getInstance()
                            .getParentNodeId(newTokens, nodeKey.getProjectName(), nodeKey.getLevel()+1, maxDist);
                    node.setParent(parentNodeKey);
                }

                //update the tree node(parent Id and pattern) by key
                PatternLevelTree.getInstance().updateNode(nodeKey, node);

                //For test
                Map<String, String> jsonItems = new HashMap<>();
                if (parentNodeKey != null) {
                    jsonItems.put(Constants.FIELD_PATTERNID, parentNodeKey.toString());
                } else {
                    jsonItems.put(Constants.FIELD_PATTERNID, "");
                }
                jsonItems.put(Constants.FIELD_REPRESENTTOKENS,
                        String.join(Constants.PATTERN_NODE_KEY_DELIMITER, node.getRepresentTokens()));
                jsonItems.put(Constants.FIELD_PATTERNTOKENS,
                        String.join(Constants.PATTERN_NODE_KEY_DELIMITER, node.getPatternTokens()));
                Text reduceOutValue = new Text();
                reduceOutValue.set(gson.toJson(jsonItems));
                context.write(NullWritable.get(), reduceOutValue);
            } catch (Exception e) {
                System.out.println("key: "+key.toString());
            }
        }
    }

   public static List<String> retrievePattern(List<String> sequence1, List<String> sequence2) throws Exception {
        List<String> alignedSequence = aligner.traceBack(sequence1, sequence2,
                aligner.createScoreMatrix(sequence1, sequence2));
        return ListUtil.removeExcessiveDuplicates(alignedSequence, "*", 5);
    }
}
