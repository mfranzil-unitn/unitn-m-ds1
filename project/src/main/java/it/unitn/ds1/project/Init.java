package it.unitn.ds1.project;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.project.message.WelcomeMsg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Init {
    final static int N_CLIENTS = 1;
    final static int N_COORDINATORS = 5;
    final static int MAX_KEYSTORE = 8192;

    public static void main(String[] args) {

        // Create the actor system
        final ActorSystem system = ActorSystem.create("project");

        // [REMOVE] When you create your ActorSystem, you can also create any number of clients.
        List<ActorRef> clientGroup = new ArrayList<>();
        for (int i = 0; i < N_CLIENTS; i++) {
            clientGroup.add(system.actorOf(TxnClient.props(i)));
        }

        List <ActorRef> coordinatorGroup = new ArrayList<>();
        for (int i = 0; i < N_COORDINATORS; i++) {
            // TODO create coordinators
            //coordinatorGroup.add(system.actorOf(TxnCoord.props(i)));
        }


        // [REMOVE] Once you have initialized the rest of the system, start the
        // [REMOVE] clients. To do so, send to the clients a WelcomeMsg including:
        // [REMOVE] - ActorRefs of the coordinators
        // [REMOVE] - Maximum key in the store
        // [REMOVE] You can stop the client actor by sending it a StopMsg (see the provided code).

        // Send welcome messages to clients with actor refs of the coordinators and max key in store

        for (int i = 0; i < N_CLIENTS; i++) {
            WelcomeMsg msg = new WelcomeMsg(MAX_KEYSTORE, coordinatorGroup);
            clientGroup.get(i).tell(msg, ActorRef.noSender());
        }

        // TODO
        // [REMOVE] You can stop the client actor by sending it a StopMsg (see the
        // [REMOVE] provided code).

        /*

                public static void main(String[] args) {

            // Create the actor system
            final ActorSystem system = ActorSystem.create("helloakka");

            // Create the coordinator
            ActorRef coordinator = system.actorOf(Coordinator.props(), "coordinator");

            // Create participants
            List<ActorRef> group = new ArrayList<>();
            for (int i = 0; i < N_PARTICIPANTS; i++) {
                group.add(system.actorOf(TxnDSS.props(i), "participant" + i));
            }

            // Send start messages to the participants to inform them of the group
            StartMessage start = new StartMessage(group);
            for (ActorRef peer : group) {
                peer.tell(start, null);
            }

            // Send the start messages to the coordinator
            coordinator.tell(start, null);

            try {
                System.out.println(">>> Press ENTER to exit <<<");
                System.in.read();
            } catch (IOException ignored) {
            }
            system.terminate();
        }

        // Create a "virtual synchrony manager"
        ActorRef manager = system.actorOf(VirtualSynchManager.props(), "vsmanager");

        // Send join messages to the manager and the nodes to inform them of the whole group
        JoinGroupMsg start = new JoinGroupMsg(group);
        manager.tell(start, ActorRef.noSender());
        for (ActorRef peer : group) {
            peer.tell(start, ActorRef.noSender());
        }

        inputContinue();

        // Create new nodes and make them join the existing group
        ActorRef joiningFirst = system.actorOf(VirtualSynchActor.props(manager, true), "vsnodeJ0");
        ActorRef joiningSecond = system.actorOf(VirtualSynchActor.props(manager, true), "vsnodeJ1");

        inputContinue();

        // Make one of the new nodes crash while sending stabilization,
        // and the other while sending the flush (which will occur due to the first crash);
        // nextCrashAfter in CrashMsg controls how many messages are correctly sent before crashing
        joiningFirst.tell(new CrashMsg(CrashType.ChatMsg, 2), ActorRef.noSender());
        //joiningSecond.tell(new CrashMsg(CrashType.ViewFlushMsg, 0), ActorRef.noSender());

        inputContinue();

        // Restart nodes (they will join again through the manager)
        joiningFirst.tell(new RecoveryMsg(), ActorRef.noSender());
        //joiningSecond.tell(new RecoveryMsg(), ActorRef.noSender());

        inputContinue();

        // Make one of the new nodes crash while sending stabilization,
        // and the other while sending the flush (which will occur due to the first crash);
        // nextCrashAfter in CrashMsg controls how many messages are correctly sent before crashing
        joiningFirst.tell(new CrashMsg(CrashType.ChatMsg, 1), ActorRef.noSender());
        //joiningSecond.tell(new CrashMsg(CrashType.ViewFlushMsg, 1), ActorRef.noSender());

        inputContinue();

        // Restart nodes (they will join again through the manager)
        joiningFirst.tell(new RecoveryMsg(), ActorRef.noSender());
        //joiningSecond.tell(new RecoveryMsg(), ActorRef.noSender());

        inputContinue();


         */
        // system shutdown
        system.terminate();
    }

    public static void inputContinue() {
        try {
            System.out.println(">>> Press ENTER to continue <<<");
            System.in.read();
        } catch (IOException ignored) {
        }
    }
}
