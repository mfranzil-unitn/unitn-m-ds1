package it.unitn.ds1.project.message.dss;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Start message that sends the list of participants to everyone
public class StartMessage implements Serializable {
    public final List<ActorRef> group;

    public StartMessage(List<ActorRef> group) {
        this.group = Collections.unmodifiableList(new ArrayList<>(group));
    }
}
