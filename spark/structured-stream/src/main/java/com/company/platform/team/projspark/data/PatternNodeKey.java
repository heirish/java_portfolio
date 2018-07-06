package com.company.platform.team.projspark.data;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

/**
 * Created by admin on 2018/7/6.
 */
public class PatternNodeKey extends PatternLevelKey implements Comparable<PatternNodeKey>{
    private String Id;
    private static final String DELIMITER = "%@%";

    public PatternNodeKey(String projectName, int level, String Id) {
        super(projectName, level);
        this.Id = Id;
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
        return (super.equals(o) && StringUtils.equals(this.Id, nodeKey.Id));
    }

    //https://stackoverflow.com/questions/113511/best-implementation-for-hashcode-method
    @Override
    public int hashCode()
    {
        int hash = super.hashCode();
        if (this.Id != null) {
            hash += 31 * this.Id.hashCode();
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

        return this.Id.compareTo(o.Id);
    }

    public String getId(){
        return Id;
    }

    public PatternLevelKey getLevelKey() {
        return new PatternLevelKey(this.getProjectName(), this.getLevel());
    }

    public String toString() {
        return String.format("%s%s%s",
                super.toString(), DELIMITER, this.Id);
    }

    public static PatternNodeKey fromString(String key) throws Exception{
        String[] items = key.split(DELIMITER);
        System.out.println(Arrays.toString(items));
        try {
            return new PatternNodeKey(items[0], Integer.parseInt(items[1]), items[2]);
        } catch (Exception e) {
            e.printStackTrace();
            //throw new Exception("invalid key");
        }
        return null;
    }
}
