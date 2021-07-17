package it.unitn.ds1.project.message;

import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DSSWelcomeMsg {
    public final List<ActorRef> dss;

    public DSSWelcomeMsg(List<ActorRef> dss) {
        this.dss = Collections.unmodifiableList(new ArrayList<>(dss));
    }

}
