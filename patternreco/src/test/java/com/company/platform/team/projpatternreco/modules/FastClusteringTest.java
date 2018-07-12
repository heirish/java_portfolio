package com.company.platform.team.projpatternreco.modules;

import com.company.platform.team.projpatternreco.common.preprocess.Tokenizer;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.List;

/**
 * Created by admin on 2018/6/25.
 */
public class FastClusteringTest {
    private static List<String> tokens1 = Tokenizer.simpleTokenize("This is a test String");
    private static List<String> tokens2 = Tokenizer.simpleTokenize("This is a test String");
    private static final Logger logger = Logger.getLogger("");


    @Test
    public void distanceCalculateTest() {
        boolean ret = FastClustering.belongsToCluster(tokens1, tokens2, 0.1);
        logger.info("belones to Cluster: " + ret);
    }

}
