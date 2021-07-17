package it.unitn.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class TxnAbstractNode extends AbstractActor {
    final static int VOTE_TIMEOUT = 2000;      // timeout for the votes, ms
    final static int DECISION_TIMEOUT = 2000;  // timeout for the decision, ms
    final static int N_PARTICIPANTS = 10;


        protected int id;                           // node ID
        protected List<ActorRef> participants;      // list of participant nodes
        protected Decision decision = null;         // decision taken by this node

        public TxnAbstractNode(int id) {
            super();
            this.id = id;
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
        void crash(int recoverIn) {
            getContext().become(crashed());
            print("CRASH!!!");

            // setting a timer to "recover"
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(recoverIn, TimeUnit.MILLISECONDS),
                    getSelf(),
                    new Recovery(), // message sent to myself
                    getContext().system().dispatcher(), getSelf()
            );
        }

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
        void setTimeout(int time) {
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(time, TimeUnit.MILLISECONDS),
                    getSelf(),
                    new Timeout(), // the message to send
                    getContext().system().dispatcher(), getSelf()
            );
        }

        // fix the final decision of the current node
        void fixDecision(Decision d) {
            if (!hasDecided()) {
                this.decision = d;
                print("decided " + d);
            }
        }

        boolean hasDecided() {
            return decision != null;
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
            if (hasDecided())
                getSender().tell(new DecisionResponse(decision), getSelf());

            // just ignoring if we don't know the decision
        }

}


// Start message that sends the list of participants to everyone
class StartMessage implements Serializable {
    public final List<ActorRef> group;

    public StartMessage(List<ActorRef> group) {
        this.group = Collections.unmodifiableList(new ArrayList<>(group));
    }
}

enum Vote {NO, YES}

enum Decision {ABORT, COMMIT}

class VoteRequest implements Serializable {

    public final String transactionID;

    public VoteRequest(String tID){
        this.transactionID = tID;
    }

}

class VoteResponse implements Serializable {
    public final String transactionID;
    public final Vote vote;

    public VoteResponse(String transactionID, Vote v) {
        this.transactionID = transactionID;
        vote = v;
    }
}

class DecisionRequest implements Serializable {

    public final String transactionID;

    public DecisionRequest(String transactionID){
        this.transactionID = transactionID;
    }

}

class DecisionResponse implements Serializable {
    public final Decision decision;
    public final String transactionID;

    public DecisionResponse(String transactionID, Decision d) {
        this.transactionID = transactionID;
        this.decision = d;
    }
}

class Timeout implements Serializable {
}

class Recovery implements Serializable {
}