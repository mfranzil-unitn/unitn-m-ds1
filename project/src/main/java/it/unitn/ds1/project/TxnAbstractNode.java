package it.unitn.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.ds1.project.message.dss.*;
import it.unitn.ds1.project.message.dss.decision.Decision;
import it.unitn.ds1.project.message.dss.decision.DecisionRequest;
import it.unitn.ds1.project.message.dss.decision.DecisionResponse;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.List;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class TxnAbstractNode extends AbstractActor {
    final static int VOTE_TIMEOUT = 2000;      // timeout for the votes, ms
    final static int DECISION_TIMEOUT = 2000;  // timeout for the decision, ms
    final static int N_PARTICIPANTS = 10;


    protected int id;                           // node ID
    protected List<ActorRef> participants;      // list of participant nodes
    protected Map<String, Decision> decision = null;

    public TxnAbstractNode(int id) {
        super();
        this.id = id;
        this.decision = new HashMap<>();
    }

    // abstract method to be implemented in extending classes
    protected abstract void onRecovery(Recovery msg);

    void setGroup(StartMessage sm) {
        participants = new ArrayList<>();
        for (ActorRef b : sm.group) {
            if (!b.equals(getSelf())) {

                // copying all participant refs except for self
                this.participants.add(b);
            }
        }
        print("starting with " + sm.group.size() + " peer(s)");
    }

    // emulate a crash and a recovery in a given time
    abstract void crash(int recoverIn);

    // emulate a delay of d milliseconds
    void delay(int d) {
        try {
            Thread.sleep(d);
        } catch (Exception ignored) {
        }
    }

    void multicast(Serializable m) {
        for (ActorRef p : participants)
            p.tell(m, getSelf());
    }

    // a multicast implementation that crashes after sending the first message
    void multicastAndCrash(Serializable m, int recoverIn) {
        for (ActorRef p : participants) {
            p.tell(m, getSelf());
            crash(recoverIn);
            return;
        }
    }

    // schedule a Timeout message in specified time
    void setTimeout(String transactionID, int time) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Timeout(transactionID), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    // fix the final decision of the current node
    void fixDecision(String transactionID, Decision d) {
        if (!hasDecided(transactionID)) {
            this.decision.put(transactionID, d);
            print("decided " + d);
        }
    }

    boolean hasDecided(String transactionID) {
        return decision.get(transactionID) != null;
    } // has the node decided?

    // a simple logging function
    void print(String s) {
        System.out.format("%2d: %s\n", id, s);
    }

    @Override
    public Receive createReceive() {
        // Empty mapping: we'll define it in the inherited classes
        return receiveBuilder().build();
    }

    public Receive crashed() {
        return receiveBuilder()
                .match(Recovery.class, this::onRecovery)
                .matchAny(msg -> {
                })
                .build();
    }

    public void onDecisionRequest(DecisionRequest msg) {  /* Decision Request */
        if (hasDecided(msg.transactionID)) {
            getSender().tell(new DecisionResponse(msg.transactionID, decision.get(msg.transactionID)), getSelf());
        }

        // just ignoring if we don't know the decision
    }

}


