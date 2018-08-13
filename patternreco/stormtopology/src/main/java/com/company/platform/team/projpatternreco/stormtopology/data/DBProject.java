package com.company.platform.team.projpatternreco.stormtopology.data;

/**
 * Created by admin on 2018/8/13.
 */
public class DBProject {
    private int id;
    private String projectName;
    private int leafMax;

    public int getId() {
        return this.id;
    }
    public void setId(int id) {
       this.id = id;
    }

    public String getProjectName() {
        return this.projectName;
    }
    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public int getLeafMax() {
        return this.leafMax;
    }
    public void setLeafMax(int leafMax) {
        this.leafMax = leafMax;
    }

    public String toString(){
        return String.format("id:%s, projectName:%s, leafMax:%s",
                id, projectName, leafMax);
    }
}
