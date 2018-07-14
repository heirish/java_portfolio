package com.company.platform.team.projpatternreco.common.data;
import org.apache.commons.lang.StringUtils;

import java.util.UUID;

/**
 * Created by admin on 2018/7/6.
 */
public class PatternNodeKey implements Comparable<PatternNodeKey>{
    private String id;
    private PatternLevelKey levelKey;
    private static final String DELIMITER = "#@#";

    public PatternNodeKey(String projectName, int level) {
        this.id= UUID.randomUUID().toString().replace("-", "");
        this.levelKey = new PatternLevelKey(projectName, level);
    }

    public PatternNodeKey(PatternLevelKey levelKey) {
        this.id= UUID.randomUUID().toString().replace("-", "");
        this.levelKey = levelKey;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
        {
            return true;
        }
        if (this.getClass() != o.getClass())
            return false;
        PatternNodeKey nodeKey= (PatternNodeKey)o;
        return (this.levelKey.equals(nodeKey.getLevelKey()) && StringUtils.equals(this.id, nodeKey.id));
    }

    //https://stackoverflow.com/questions/113511/best-implementation-for-hashcode-method
    @Override
    public int hashCode()
    {
        int hash = this.levelKey.hashCode();
        if (this.id != null) {
            hash += 31 * this.id.hashCode();
        }
        return hash;
    }

    @Override
    public int compareTo(PatternNodeKey o) {
        int num = this.getProjectName().compareTo(o.getProjectName());
        if (num != 0) {
            return num;
        }

        num = this.getLevel() - o.getLevel();
        if (num != 0) {
            return num;
        }

        return this.id.compareTo(o.id);
    }

    public String getProjectName() {
        return this.levelKey.getProjectName();
    }

    public int getLevel() {
        return this.levelKey.getLevel();
    }

    public PatternLevelKey getLevelKey() {
        return this.levelKey;
    }

    public String toString() {
        return String.format("%s%s%s",
                this.levelKey.toString(), DELIMITER, this.id);
    }

    public static PatternNodeKey fromString(String key) throws Exception{
        String[] items = key.split(DELIMITER);
        try {
            PatternLevelKey levelKey = PatternLevelKey.fromString(items[0]);
            PatternNodeKey nodeKey = new PatternNodeKey(levelKey);
            nodeKey.id = items[2];
            return nodeKey;
        } catch (Exception e) {
            throw new Exception("invalid key", e);
        }
    }
}
