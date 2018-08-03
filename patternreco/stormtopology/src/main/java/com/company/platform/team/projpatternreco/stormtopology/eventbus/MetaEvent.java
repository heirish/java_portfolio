package com.company.platform.team.projpatternreco.stormtopology.eventbus;

import com.company.platform.team.projpatternreco.stormtopology.utils.PatternMetaType;

/**
 * Created by admin on 2018/8/3.
 */
public class MetaEvent implements IEventType {
    private EventBus eventBus;

    @Override
    public EventBus getEventBus() {
        return this.eventBus;
    }

    @Override
    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }
}
