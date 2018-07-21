package com.company.platform.team.projpatternreco.common.data;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

/**
 * Created by admin on 2018/7/6.
 */
public final class PatternNodeKey implements Comparable<PatternNodeKey>{
    private static final String DELIMITER = "#@#";

    private String id;
    private PatternLevelKey levelKey;

    private int hashCode;

    public PatternNodeKey(String projectName, int level) {
        this.id= UUID.randomUUID().toString().replace("-", "");
        this.levelKey = new PatternLevelKey(projectName, level);
        hashCode = getHashCode();
    }

    public PatternNodeKey(PatternLevelKey levelKey) {
        this.id= UUID.randomUUID().toString().replace("-", "");
        this.levelKey = levelKey;
        hashCode = getHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
        {
            return true;
        }
        if(o == null) {
            return false;
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
        return hashCode;
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
        try {
            int pos = key.lastIndexOf(DELIMITER);
            PatternLevelKey levelKey = PatternLevelKey.fromString(key.substring(0, pos));
            PatternNodeKey nodeKey = new PatternNodeKey(levelKey);
            nodeKey.id = key.substring(pos + DELIMITER.length());
            nodeKey.hashCode = nodeKey.getHashCode();
            return nodeKey;
        } catch (Exception e) {
            throw new Exception("invalid key: " + key, e);
        }
    }

    private int getHashCode() {
        int hash = this.levelKey.hashCode();
        if (this.id != null) {
            hash += 31 * this.id.hashCode();
        }
        return hash;
    }
}
