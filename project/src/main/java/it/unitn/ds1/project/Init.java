package it.unitn.ds1.project;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.project.message.ClientWelcomeMsg;
import it.unitn.ds1.project.message.CoordinatorWelcomeMsg;
import it.unitn.ds1.project.message.DSSWelcomeMsg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Init {
    final static int N_CLIENTS = 1;
    final static int N_COORDINATORS = 2;
    final static int MAX_KEYSTORE = 99;
    final static int N_DATASTORE = (MAX_KEYSTORE - 9) / 10;

    public static void main(String[] args) {

        // Create the actor system
        final ActorSystem system = ActorSystem.create("project");

        List<ActorRef> clientGroup = new ArrayList<>();
        for (int i = 0; i < N_CLIENTS; i++) {
            System.out.println("Generating Client actor with ID " + i);
            clientGroup.add(system.actorOf(Client.props(i)));
        }

        List<ActorRef> coordinatorGroup = new ArrayList<>();
        for (int i = 100; i - 100 < N_COORDINATORS; i++) {
            System.out.println("Generating Coordinator actor with ID " + i);
            coordinatorGroup.add(system.actorOf(Coordinator.props(i)));
        }

        List<ActorRef> dataStoreGroup = new ArrayList<>();
        for (int i = 1000; i - 1000 <= N_DATASTORE; i++) {
            int lowerBound = (i - 1000) * 10;
            System.out.println("Generating Datastore actor with ID " + i + " and lower bound " + lowerBound);
            dataStoreGroup.add(system.actorOf(DSS.props(i, lowerBound)));
        }

        // Tell all coordinators the group of datastores
        for (int j = 0; j < N_COORDINATORS; j++) {
            CoordinatorWelcomeMsg msg = new CoordinatorWelcomeMsg(dataStoreGroup);
            coordinatorGroup.get(j).tell(msg, ActorRef.noSender());
        }

        // Also tell all datastores the group of datastores
        for (int j = 0; j < N_DATASTORE; j++) {
            DSSWelcomeMsg msg = new DSSWelcomeMsg(dataStoreGroup);
            dataStoreGroup.get(j).tell(msg, ActorRef.noSender());
        }

        // Tell all clients the group of coordinators and the max keystore
        for (int j = 0; j < N_CLIENTS; j++) {
            ClientWelcomeMsg msg = new ClientWelcomeMsg(MAX_KEYSTORE, coordinatorGroup);
            clientGroup.get(j).tell(msg, ActorRef.noSender());
        }

        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ignored) {
        }

    }

}
