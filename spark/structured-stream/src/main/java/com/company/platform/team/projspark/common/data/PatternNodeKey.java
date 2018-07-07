package com.company.platform.team.projspark.common.data;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.UUID;

/**
 * Created by admin on 2018/7/6.
 */
public class PatternNodeKey extends PatternLevelKey implements Comparable<PatternNodeKey>{
    private String id;

    public PatternNodeKey(String projectName, int level) {
        super(projectName, level);
        this.id= UUID.randomUUID().toString().replace("-", "");
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
        return (super.equals(o) && StringUtils.equals(this.id, nodeKey.id));
    }

    //https://stackoverflow.com/questions/113511/best-implementation-for-hashcode-method
    @Override
    public int hashCode()
    {
        int hash = super.hashCode();
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

    private void setId(String id) {
        this.id = id;
    }

    public String getId(){
        return id;
    }

    public PatternLevelKey getLevelKey() {
        return new PatternLevelKey(this.getProjectName(), this.getLevel());
    }

    public String toString() {
        return String.format("%s%s%s",
                super.toString(), getDelimiter(), this.id);
    }

    public static PatternNodeKey fromString(String key) throws Exception{
        String[] items = key.split(getDelimiter());
        System.out.println(Arrays.toString(items));
        try {
            PatternNodeKey nodeKey = new PatternNodeKey(items[0], Integer.parseInt(items[1]));
            nodeKey.setId(items[2]);
            return nodeKey;
        } catch (Exception e) {
            throw new Exception("invalid key", e);
        }
    }
}
