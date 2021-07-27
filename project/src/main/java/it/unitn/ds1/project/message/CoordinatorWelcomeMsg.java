package it.unitn.ds1.project.message;

import akka.actor.ActorRef;

import java.util.List;

public class CoordinatorWelcomeMsg {
    public final List<ActorRef> dss;

    public CoordinatorWelcomeMsg(List<ActorRef> dss) {
        this.dss = List.copyOf(dss);
    }

}
