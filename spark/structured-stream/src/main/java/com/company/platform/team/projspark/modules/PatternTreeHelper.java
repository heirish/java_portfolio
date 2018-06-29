package com.company.platform.team.projspark.modules;

import com.company.platform.team.projspark.data.Constants;

import java.util.List;

/**
 * Created by admin on 2018/6/29.
 */
public class PatternTreeHelper {

    //findParent and setNodeParent runs when fast clustering

    //setNodePattern runs when pattern retrieve

    //deleteNode runs when clear data

    //synchronize is to send tree to local cache, runs when findParent added a new parent
    //does master need to know which server has which tree expired?
    public String findParent(int level, List<String> tokens) {
        //TODO:distance decay according to level, distancemanger
        //user can only change the distance through configure, can't set it in running time.
        double maxdistance = Constants.PATTERN_LEAF_MAXDIST;
        //get all the level nodes
        //calculate the distance
        //if exists, return nodeId
        //else, add a new node this level of the tree
        return "";
    }

    public void setNodeParent(int level, String nodeId, String parent) {

    }



}
