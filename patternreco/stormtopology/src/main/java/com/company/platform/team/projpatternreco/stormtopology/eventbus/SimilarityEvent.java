package com.company.platform.team.projpatternreco.stormtopology.eventbus;

/**
 * Created by admin on 2018/8/3.
 */
public class SimilarityEvent implements IEventType{
    private EventBus eventBus;
    private String projectName;

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getProjectName() {
        return this.projectName;
    }

    @Override
    public EventBus getEventBus() {
        return this.eventBus;
    }

    @Override
    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }
}
