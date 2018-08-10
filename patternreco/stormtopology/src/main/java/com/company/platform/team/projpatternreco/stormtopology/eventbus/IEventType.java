package com.company.platform.team.projpatternreco.stormtopology.eventbus;

/**
 * Created by admin on 2018/8/3.
 */
public interface IEventType {
    EventBus getEventBus();
    void setEventBus(EventBus eventBus);
}
