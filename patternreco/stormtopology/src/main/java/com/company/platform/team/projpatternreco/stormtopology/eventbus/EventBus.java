package com.company.platform.team.projpatternreco.stormtopology.eventbus;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by admin on 2018/8/3.
 */
public class EventBus {
    private final Set<IEventListener> listeners = new HashSet<>();
    private static final EventBus instance = new EventBus();

    public static EventBus getInstance() {
        return instance;
    }

    public void registerListener(IEventListener listener) {
        this.listeners.add(listener);
    }

    public void unregisterListener(IEventListener listener) {
        this.listeners.remove(listener);
    }

    public void publish(IEventType event) {
        event.setEventBus(this);
        this.listeners.forEach(listener->listener.accept(event));
    }
}
