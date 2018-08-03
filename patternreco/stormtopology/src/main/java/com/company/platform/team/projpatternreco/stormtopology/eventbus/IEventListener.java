package com.company.platform.team.projpatternreco.stormtopology.eventbus;

import java.util.function.Consumer;

/**
 * Created by admin on 2018/8/3.
 */
public interface IEventListener extends Consumer<IEventType> {
    void accept(IEventType event);
}
