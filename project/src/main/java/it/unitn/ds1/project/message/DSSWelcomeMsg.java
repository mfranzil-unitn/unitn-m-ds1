package it.unitn.ds1.project.message;

import akka.actor.ActorRef;

import java.util.List;

public class DSSWelcomeMsg {
    public final List<ActorRef> dss;

    public DSSWelcomeMsg(List<ActorRef> dss) {
        this.dss = List.copyOf(dss);
    }

}
