package it.unitn.ds1.project;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.common.Log;
import it.unitn.ds1.common.LogLevel;
import it.unitn.ds1.project.message.ClientWelcomeMsg;
import it.unitn.ds1.project.message.CoordinatorWelcomeMsg;
import it.unitn.ds1.project.message.DSSWelcomeMsg;
import it.unitn.ds1.project.message.dss.RequestSummaryMsg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Init {
    final static int N_CLIENTS = 10;
    final static int N_COORDINATORS = 15;
    final static int N_DATASTORE = 100;
    final static int MAX_KEYSTORE = N_DATASTORE * 10 - 1;

    // Useless for us
    // final static boolean CRASH_COORDINATOR_BEFORE_VOTE_REQUEST = false;

    final static boolean CRASH_COORDINATOR_AFTER_ONE_VOTE_REQUEST = false;
    final static boolean CRASH_COORDINATOR_AFTER_ALL_VOTE_REQUEST = false;

    final static boolean CRASH_COORDINATOR_BEFORE_DECISION_RESPONSE = false;

    final static boolean CRASH_COORDINATOR_AFTER_ONE_DECISION_RESPONSE = false;
    final static boolean CRASH_COORDINATOR_AFTER_ALL_DECISION_RESPONSE = false;

    // the dss who crashes is always #1000
    final static boolean CRASH_DSS_BEFORE_VOTE_RESPONSE = false;
    final static boolean CRASH_DSS_BEFORE_DECISION_RESPONSE = false;


    public static void main(String[] args) throws IOException {
        // Logging
        Log.initializeLog(LogLevel.BASIC);

        // Create the actor system
        final ActorSystem system = ActorSystem.create("project");

        List<ActorRef> clientGroup = new ArrayList<>();
        for (int i = 0; i < N_CLIENTS; i++) {
            Log.log(LogLevel.DEBUG, -1, "Generating Client actor with ID " + i);
            clientGroup.add(system.actorOf(Client.props(i)));
        }

        List<ActorRef> coordinatorGroup = new ArrayList<>();
        for (int i = 100; i - 100 < N_COORDINATORS; i++) {
            Log.log(LogLevel.DEBUG, -1, "Generating Coordinator actor with ID " + i);
            coordinatorGroup.add(system.actorOf(Coordinator.props(i)));
        }

        List<ActorRef> dataStoreGroup = new ArrayList<>();
        for (int i = 1000; i - 1000 < N_DATASTORE; i++) {
            int lowerBound = (i - 1000) * 10;
            Log.log(LogLevel.DEBUG, -1, "Generating Datastore actor with ID " + i + " and lower bound " + lowerBound);
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

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.flush();
            String response;

            label:
            while (true) {
                System.out.println("To verify consistency, wait until all transactions " +
                        "have ended and press [y], else [n]. If you wish to stop, press [s].");
                response = scanner.nextLine();

                switch (response) {
                    case "y":
                    case "Y":
                        for (int j = 0; j < N_DATASTORE; j++) {
                            RequestSummaryMsg msg = new RequestSummaryMsg();
                            dataStoreGroup.get(j).tell(msg, ActorRef.noSender());
                        }
                        break label;
                    case "s":
                    case "S":
                        system.terminate();
                        break label;
                    case "n":
                    case "N":
                        break label;
                }
            }

            do {
                System.out.println("Press [c] to continue with the next step.");
                response = scanner.nextLine();
            } while (!response.equals("c") && !response.equals("C"));

            for (int j = 0; j < N_CLIENTS; j++) {
                ClientWelcomeMsg msg = new ClientWelcomeMsg(MAX_KEYSTORE, coordinatorGroup);
                clientGroup.get(j).tell(msg, ActorRef.noSender());
            }
        }

    }

}
