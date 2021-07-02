package it.unitn.ds1.multicast;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

class Chatter extends AbstractActor {

    // number of chat messages to send
    final static int N_MESSAGES = 10;
    private final Random rnd = new Random();
    private List<ActorRef> group; // the list of peers (the multicast group)
    private int sendCount = 0;    // number of sent messages
    private final String myTopic;       // The topic I am interested in, null if no topic
    private final int id;         // ID of the current actor
    private int[] vc;             // the local vector clock

    // a buffer storing all received chat messages
    private final StringBuffer chatHistory = new StringBuffer();

    private final ConcurrentLinkedDeque<ChatMsg> buffer = new ConcurrentLinkedDeque<>();

    /*public final boolean isVcLess(int[] vc1, int[] vc2) {
        assert (vc1.length == vc2.length);
        boolean res = true;

        for (int i = 0; i < vc1.length; i++) {
            if (vc1[i] > vc2[i]) {
                res = false;
                break;
            }
        }

        return res;
    }

    public final boolean isVcConcurrent(int[] vc1, int[] vc2) {
        return !isVcLess(vc1, vc2) && !isVcLess(vc2, vc1);
    }
*/
    // TODONE 2: provide a buffer for out-of-order messages

    /* -- Message types ------------------------------------------------------- */

    // Start message that informs every chat participant about its peers
    public static class JoinGroupMsg implements Serializable {
        private final List<ActorRef> group; // list of group members

        public JoinGroupMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    // A message requesting the peer to start a discussion on his topic
    public static class StartChatMsg implements Serializable {
    }

    // Chat message
    public static class ChatMsg implements Serializable {
        public final String topic;   // "topic" of the conversation
        public final int n;          // the number of the reply in the current topic
        public final int senderId;   // the ID of the message sender
        public final int[] vc;       // vector clock

        public ChatMsg(String topic, int n, int senderId, int[] vc) {
            this.topic = topic;
            this.n = n;
            this.senderId = senderId;
            this.vc = new int[vc.length];
            for (int i = 0; i < vc.length; i++) this.vc[i] = vc[i];
        }

        @Override
        public String toString() {
            return "ChatMsg{" +
                    "topic='" + topic + '\'' +
                    ", n=" + n +
                    ", senderId=" + senderId +
                    ", vc=" + Arrays.toString(vc) +
                    '}';
        }
    }

    // A message requesting to print the chat history
    public static class PrintHistoryMsg implements Serializable {
    }

    /* -- Actor constructor --------------------------------------------------- */

    public Chatter(int id, String topic) {
        this.id = id;
        this.myTopic = topic;
    }

    static public Props props(int id, String topic) {
        return Props.create(Chatter.class, () -> new Chatter(id, topic));
    }

    /* -- Actor behaviour ----------------------------------------------------- */

    private void sendChatMsg(String topic, int n) {
        sendCount++;
        this.vc[id]++;

        // TODONE 3: update vector clock

        // generate chat message
        ChatMsg m = new ChatMsg(topic, n, this.id, this.vc);
        System.out.printf("%02d: %s%02d\n", this.id, topic, n);

        // send to peers and append to log
        multicast(m);
        appendToHistory(m);
    }

    private void multicast(Serializable m) {
        // randomly arrange peers
        List<ActorRef> shuffledGroup = new ArrayList<>(group);
        Collections.shuffle(shuffledGroup);

        // multicast to all peers in the group (do not send any message to self)
        for (ActorRef p : shuffledGroup) {
            if (!p.equals(getSelf())) {
                p.tell(m, getSelf());

                // simulate network delays using sleep
                try {
                    Thread.sleep(rnd.nextInt(1));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(StartChatMsg.class, this::onStartChatMsg)
                .match(ChatMsg.class, this::onChatMsg)
                .match(PrintHistoryMsg.class, this::printHistory)
                .build();
    }

    private void onJoinGroupMsg(JoinGroupMsg msg) {
        this.group = msg.group;

        // create the vector clock
        this.vc = new int[this.group.size()];
        System.out.printf("%s: joining a group of %d peers with ID %02d\n",
                getSelf().path().name(), this.group.size(), this.id);
    }

    private void onStartChatMsg(StartChatMsg msg) {
        // start topic with message 0
        sendChatMsg(myTopic, 0);
    }

    private void onChatMsg(ChatMsg msg) {
        // "deliver" the message to the simulated chat user
        /*System.out.println("Actor " + id + " on " + msg.topic + msg.n + ":\n"
                + "    this vc: " + Arrays.toString(this.vc) + "\n"
                + "    message vc: " + Arrays.toString(msg.vc) + "\n"
                + "    offset: " + isDeliverable(msg));*/

        emptyBuffer();
        if (isDeliverable(msg)) {
            deliver(msg);
        } else {
            //System.out.println("Adding to buffer...");
            buffer.add(msg);
        }
        emptyBuffer();

        // TODONE 4: deliver only if the message is in-order
        // TODONE 5 hint: once a message is delivered, update the vector clock...

    }

    private void deliver(ChatMsg m) {
        updateClock(m);
        appendToHistory(m);
        //if (myTopic != null) {
        //System.out.print(m.topic.equals(myTopic) + " - " + (sendCount < N_MESSAGES) + " ---");
        //System.out.printf("Actor %d sending message %s n %d (total sent: %d)\n", id, myTopic, m.n + 1, sendCount);
        //}
        // if the message is on my topic and I still have something to say...
        if (m.topic.equals(myTopic) && sendCount < N_MESSAGES) {
            // reply to the received message with an incremented value and the same topic
            sendChatMsg(m.topic, m.n + 1);
        }

        // our "chat application" appends all the received messages to the
        // chatHistory and replies if the topic of the message is interesting

    }

    private void updateClock(ChatMsg msg) {
        for (int i = 0; i < msg.vc.length; i++) {
            this.vc[i] = Math.max(this.vc[i], msg.vc[i]);
        }
    }

    private void appendToHistory(ChatMsg m) {
        chatHistory.append(m.topic).append(m.n).append(" ");
    }

    private void printHistory(PrintHistoryMsg msg) {
        System.out.printf("%s %02d: %s\n", verifyHistoryIntegrity(), this.id, chatHistory);
    }

    private String verifyHistoryIntegrity() {
        String[] history = this.chatHistory.toString().split(" ");
        HashSet<String> topics = new HashSet<>();
        for (String s : history) {
            String currentTopic = s.split("[0-9]*")[0];
            topics.add(currentTopic);
        }
        for (String t : topics) {
            int j = 0;
            for (String s : history) {
                if (s.contains(Integer.toString(j)) && s.contains(t)) {
                    j++;
                }
            }
            if (j != 2 * N_MESSAGES) {
                return "[x]";
            }
        }
        return "[o]";
    }

    private void emptyBuffer() {
        while (true) {
            ChatMsg deliverable = null;

            for (ChatMsg m : buffer) {
                if (isDeliverable(m)) {
                    deliverable = m;
                    deliver(deliverable);
                    System.out.flush();
                    buffer.remove(m);
                }
            }

            if (deliverable == null) {
                break;
            }
            //System.out.println("Removed " + deliverable + " by actor " + id);
        }
    }

    private boolean isDeliverable(ChatMsg msg) {
        System.out.printf("act %d attempting delivery of %s\n", id, msg);
        if (msg.vc.length != this.vc.length) {
            throw new ArrayIndexOutOfBoundsException("Different array sizes");
        }

        if (msg.vc[msg.senderId] != this.vc[msg.senderId] + 1) {
            System.out.printf(" ordering failure\n");
            return false;
        }

        //int res = 0;

        for (int i = 0; i < this.vc.length; i++) {
            if (i == msg.senderId) {
                continue;
            }
            if (msg.vc[i] <= this.vc[i]) {
                continue;
            }
            System.out.printf(" vc [%d] failure\n", i);
            return false;
            //    res = res - (receiverVc[i] - messageVc[i]);
        }
        System.out.printf(" successful\n");
        return true;
        // 1 -> è giusto
        // 2 -> 1 in avanti
        //return res;
    }

}
