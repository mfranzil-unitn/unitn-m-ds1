package it.unitn.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import it.unitn.ds1.project.message.dss.DSSMessage;
import it.unitn.ds1.project.message.dss.Recovery;
import it.unitn.ds1.project.message.dss.Timeout;
import it.unitn.ds1.project.message.dss.decision.DSSDecision;
import it.unitn.ds1.project.message.dss.decision.DSSDecisionRequest;
import it.unitn.ds1.project.message.dss.decision.DSSDecisionResponse;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public abstract class AbstractNode extends AbstractActor {
    final static int VOTE_TIMEOUT = 5000;      // timeout for the votes, ms
    final static int DECISION_TIMEOUT = 5000;  // timeout for the decision, ms

    final static int CRASH_TIME = 7000;

    final static int MAX_DELAY = 100;

    protected int id;                           // node ID

    protected final Random r;

    protected Map<String, DSSDecision> decision;
    protected Map<String, Cancellable> timeouts;

    protected AbstractNode(int id) {
        this.id = id;
        this.decision = new HashMap<>();
        this.timeouts = new HashMap<>();
        this.r = new Random();

    }

    @Override
    public Receive createReceive() {
        // Empty mapping: we'll define it in the inherited classes
        return receiveBuilder().build();
    }

    protected Receive crashed() {
        return receiveBuilder()
                .match(Recovery.class, this::onRecovery)
                .matchAny(msg -> {
                })
                .build();
    }

    // abstract methods to be implemented in extending classes

    protected abstract void onRecovery(Recovery msg);
    protected abstract void onTimeout(Timeout msg);

    protected void crash(int recoverIn) {
        getContext().become(crashed());
        log(" [CRASHED]");

        // setting a timer to "recover"
        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoverIn, TimeUnit.MILLISECONDS),
                getSelf(),
                new Recovery(), // message sent to myself
                getContext().system().dispatcher(), getSelf()
        );
    }

    // multicast
    protected abstract void multicast(DSSMessage m);
    protected abstract void multicastAndCrash(DSSMessage m, int recoverIn);


    // schedule a Timeout message in specified time
    protected void setTimeout(String transactionID, int time) {
        timeouts.put(transactionID, getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Timeout(transactionID), // the message to send
                getContext().system().dispatcher(), getSelf()
        ));
    }

    // fix the final decision of the current node
    protected void fixDecision(String transactionID, DSSDecision d) {
        if (!hasDecided(transactionID)) {
            this.decision.put(transactionID, d);
            log("fixed decision " + d);
        }
    }

    protected boolean hasDecided(String transactionID) {
        return decision.get(transactionID) != null;
    } // has the node decided?

    protected void onDecisionRequest(DSSDecisionRequest msg) {  /* DSSDecision Request */
        if (hasDecided(msg.transactionID)) {
            getSender().tell(new DSSDecisionResponse(msg.transactionID, decision.get(msg.transactionID)), getSelf());
        }
        // just ignoring if we don't know the decision
    }

    /* -- Auxiliary ---------------------- */

    protected void log(String s) {
        System.out.format("%2d: %s\n", id, s);
    }

    protected void delay(int d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}


