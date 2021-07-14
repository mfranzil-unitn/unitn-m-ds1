package it.unitn.ds1.project.message;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// send this message to the client at startup to inform it about the coordinators and the keys
public class WelcomeMsg implements Serializable {
    public final Integer maxKey;
    public final List<ActorRef> coordinators;

    public WelcomeMsg(int maxKey, List<ActorRef> coordinators) {
        this.maxKey = maxKey;
        this.coordinators = Collections.unmodifiableList(new ArrayList<>(coordinators));
    }
}
