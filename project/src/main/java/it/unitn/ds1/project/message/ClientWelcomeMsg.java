package it.unitn.ds1.project.message;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

// send this message to the client at startup to inform it about the coordinators and the keys
public class ClientWelcomeMsg implements Serializable {
    public final Integer maxKey;
    public final List<ActorRef> coordinators;

    public ClientWelcomeMsg(int maxKey, List<ActorRef> coordinators) {
        this.maxKey = maxKey;
        this.coordinators = List.copyOf(coordinators);
    }
}
