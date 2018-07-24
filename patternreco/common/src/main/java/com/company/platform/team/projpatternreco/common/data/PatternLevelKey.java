package com.company.platform.team.projpatternreco.common.data;

import org.apache.commons.lang3.StringUtils;

import java.security.InvalidParameterException;

/**
 * Created by admin on 2018/7/6.
 */
public final class PatternLevelKey {
    private String projectName;
    private int level;
    private static final String DELIMITER = "#@#";

    private int hashCode;

    public PatternLevelKey(String projectName, int level) {
        this.projectName = projectName;
        this.level = level;
        this.hashCode = getHashCode();
    }

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
        PatternLevelKey nodeKey= (PatternLevelKey)o;
        return (StringUtils.equals(this.projectName, nodeKey.projectName)
        && this.level ==nodeKey.level);
    }

    //https://stackoverflow.com/questions/113511/best-implementation-for-hashcode-method
    @Override
    public int hashCode()
    {
        return hashCode;
    }

    public int getLevel() {
        return this.level;
    }

    public String getProjectName() {
        return this.projectName;
    }

    public String toString() {
        return String.format("%s%s%s",
                this.projectName, this.DELIMITER,
                this.level);
    }

    public String toDelimitedString(String delimiter) {
        String separator = StringUtils.isEmpty(delimiter) ? "" : delimiter;
        return String.format("%s%s%s",
                this.projectName, separator, this.level);
    }

    public static PatternLevelKey fromString(String key) throws Exception{
        try {
            String[] items = key.split(DELIMITER);
            return new PatternLevelKey(items[0], Integer.parseInt(items[1]));
        } catch (Exception e) {
            throw new Exception("invalid key: " + key, e);
        }
    }

    public static PatternLevelKey fromDelimitedString(String key, String delimiter) throws Exception {
        if (StringUtils.isEmpty(delimiter)) {
            throw new InvalidParameterException("invalid delimiter: " + delimiter);
        }

        try {
            String[] items = key.split(delimiter);
            return new PatternLevelKey(items[0], Integer.parseInt(items[1]));
        } catch (Exception e) {
            throw new Exception("invalid key: " + key, e);
        }
    }

    private int getHashCode() {
        int hash = 17;
        hash += 31 * this.level;
        if (this.projectName != null) {
            hash += 31 * this.projectName.hashCode();
        }
        return hash;
    }
}
