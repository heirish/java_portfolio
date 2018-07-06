package com.company.platform.team.projspark.data;

import org.apache.commons.lang.StringUtils;

/**
 * Created by admin on 2018/7/6.
 */
public class PatternLevelKey {
    private String projectName;
    private int level;
    private static final String DELIMITER = "#@#";

    public PatternLevelKey(String projectName, int level) {
        this.projectName = projectName;
        this.level = level;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (this.getClass() != o.getClass()) {
            return false;
        }
        //if(!(o instanceof PatternLevelKey))
        //{
        //    return false;
        //}
        PatternLevelKey nodeKey= (PatternLevelKey)o;
        return (StringUtils.equals(this.projectName, nodeKey.projectName)
        && this.level ==nodeKey.level);
    }

    //https://stackoverflow.com/questions/113511/best-implementation-for-hashcode-method
    @Override
    public int hashCode()
    {
        int hash = 17;
        hash += 31 * this.level;
        if (this.projectName != null) {
            hash += 31 * this.projectName.hashCode();
        }
        return hash;
    }

    public int getLevel() {
        return this.level;
    }

    public String getProjectName() {
        return this.projectName;
    }

    protected static String getDelimiter() {
        return DELIMITER;
    }

    public String toString() {
        return String.format("%s%s%s",
                this.projectName, DELIMITER,
                this.level);
    }

    public static PatternLevelKey fromString(String key) throws Exception{
        String[] items = key.split(DELIMITER);
        try {
            return new PatternLevelKey(items[0], Integer.parseInt(items[1]));
        } catch (Exception e) {
            throw new Exception("invalid node key", e);
        }
    }
}
