import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SRNode {
    private InputReader inputReader;
    private UDPReciever reciever;
    private UDPSender sender;
    private DatagramSocket socket;
    private final ExecutorService pool;
    private int srcPort;
    private int dstPort;
    private int windowSize;
    private int timeout;
    private float lossRate;

    public SRNode(int srcPort, int dstPort, int windowSize, int timeout, float lossRate) throws SocketException {
        this.srcPort = srcPort;
        this.dstPort = dstPort;
        this.windowSize = windowSize;
        this.timeout = timeout;
        this.lossRate = lossRate;

        pool = Executors.newFixedThreadPool(3);
        this.reciever = new UDPReciever(socket, this);
        this.sender = new UDPSender(new DatagramSocket(srcPort), "localhost", dstPort, this);
        this.inputReader = new InputReader(this);
    }

    public void respond(DatagramPacket packet) {
        String data = new String(packet.getData(), 0, packet.getLength());
    }

    public void send(String message) {
        this.sender.send(message);
    }

    private void run() throws SocketException {
        send();
        recieve();
        read();
    }

    private void recieve() throws SocketException {
        pool.execute(this.reciever);
    }

    private void send() throws SocketException {
        pool.execute(this.sender);
    }

    private void read() {
        pool.execute(this.inputReader);
    }

    /**
     * Client UDP sender that takes input from console, converts it to a packet, and sends it to the server.
     *
     * @author evan
     *
     */
    public class UDPSender implements Runnable {
        private final DatagramSocket socket;
        private static final int BUFFER_SIZE = 1024;
        private static final int ACK_TIMEOUT = 1000;
        private final String receiverIP;
        private final int receiverPort;
        private final SRNode handler;
        private final StringBuffer messageBuffer = new StringBuffer();

        /**
         * Send from specified client and specified socket to specified ip and port
         *
         * @param socket
         * @param recieverIP
         * @param receiverPort
         * @param handler
         */
        public UDPSender(DatagramSocket socket, String recieverIP, int receiverPort, SRNode handler) {
            this.socket = socket;
            this.receiverIP = recieverIP;
            this.receiverPort = receiverPort;
            this.handler = handler;
        }

        public void send(String message) {
            messageBuffer.append(message);
            synchronized (messageBuffer) {
                messageBuffer.notify();
            }
        }

        @Override
        public void run() {
            // Begin to send

            byte[] buffer;
            while (true) {

                //check if we have data to send
                try {
                    synchronized (messageBuffer) {
                        if (messageBuffer.length() == 0) messageBuffer.wait();
                    }
                } catch (InterruptedException e) {
                    continue;
                }

                //create packet to send
                DatagramPacket sendPacket;
                buffer = messageBuffer.charAt(index).
                try {
                    sendPacket = new DatagramPacket(buffer, buffer.length, InetAddress.getByName(receiverIP),
                            receiverPort);
                } catch (UnknownHostException e) {
                    socket.close();
                    e.printStackTrace();
                    break;
                }

                //send the packet to the server
                boolean acked = false;
                do {
                    //try to send
                    try {
                        socket.send(sendPacket);
                    } catch (IOException e) {
                        socket.close();
                        e.printStackTrace();
                        return;
                    }

                    //wait for ack, or resend packet
                    byte[] ackbuffer = new byte[BUFFER_SIZE];
                    DatagramPacket ackPacket = new DatagramPacket(ackbuffer, ackbuffer.length);
                    try {
                        socket.setSoTimeout(ACK_TIMEOUT);
                        socket.receive(ackPacket);
                        /*
                         * String ip = ackPacket.getAddress().getHostAddress(); int port = ackPacket.getPort(); Payload
                         * payload = new Payload(new String(ackbuffer, 0, ackPacket.getLength()));
                         * System.out.println("[" + Calendar.getInstance().getTimeInMillis() +
                         * "] Receive from sender (IP: " + ip + ", Port: " + String.valueOf(port) + "): " + payload);
                         */
                        acked = true;
                    } catch (SocketTimeoutException e) {
                        //no ack was received, need to try again.
                        acked = false;
                        System.out.println("no ack received! resending packet......");
                    } catch (IOException e) {
                        socket.close();
                        e.printStackTrace();
                        return;
                    }
                } while (!acked);

                /*
                 * System.out.println("[" + Calendar.getInstance().getTimeInMillis() + "] Sent to server: (IP: " +
                 * receiverIP + ", Port: " + String.valueOf(receiverPort) + "): " + inputString);
                 */
            }

            socket.close();

        }

    }

    /**
     * Client UDP reciever that receives packets from the server and notifies the client
     *
     * @author evan
     *
     */
    public class UDPReciever implements Runnable {
        private final DatagramSocket socket;
        private final SRNode handler;
        private static final int BUFFER_SIZE = 1024;
        private static final String SUCCESS_MSG_FMT = "Receiving at port %d ...\n";

        /**
         * Receive for specified client at specified socket.
         *
         * @param socket
         * @param handler
         */
        public UDPReciever(DatagramSocket socket, SRNode handler) {
            this.socket = socket;
            this.handler = handler;
        }

        @Override
        public void run() {
            System.out.printf(SUCCESS_MSG_FMT, socket.getLocalPort());

            /*
             * Begin to receive UDP packet (no connection is set up for UDP)
             */
            byte[] buffer = new byte[BUFFER_SIZE];
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(packet);
                    /*
                     * String ip = packet.getAddress().getHostAddress(); int port = packet.getPort(); Payload cmd = new
                     * Payload(new String(buffer, 0, packet.getLength())); System.out.println("[" +
                     * Calendar.getInstance().getTimeInMillis() + "] Receive from sender (IP: " + ip + ", Port: " +
                     * String.valueOf(port) + "): " + cmd);
                     */
                    handler.respond(packet);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class InputReader implements Runnable {
        private final SRNode handler;

        public InputReader(SRNode handler) {
            this.handler = handler;
        }

        @Override
        public void run() {
            BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
            String s;

            //read input from console
            while (true) {
                try {
                    s = input.readLine();
                } catch (IOException e) {
                    socket.close();
                    e.printStackTrace();
                    continue;
                }
                handler.send(s);
            }
        }
    }

    private static String formatSendPacket(long timestamp, int packetId, char contents) {
        return String.format(SND_PKT_FMT, timestamp, Calendar.getInstance().getTimeInMillis(), packetId, contents);
    }

    private static String formatRecieveAck1(long timestamp, int packetId) {
        return String.format(RCV_ACK_1_FMT, timestamp, packetId);
    }

    private static String formatRecieveAck2(long timestamp, int packetId, int windowStart, int windowEnd) {
        return String.format(RCV_ACK_2_FMT, timestamp, packetId, windowStart, windowEnd);
    }

    private static String formatPacketTimeout(long timestamp, int packetId) {
        return String.format(PKT_TIMEOUT_FMT, timestamp, packetId);
    }

    private static String formatRecievePacket1(long timestamp, int packetId, char contents) {
        return String.format(RCV_PKT_1_FMT, timestamp, packetId, contents);
    }

    private static String formatRecievePacket2(long timestamp, int packetId, char contents, int windowStart,
            int windowEnd) {
        return String.format(RCV_PKT_2_FMT, timestamp, packetId, contents, windowStart, windowEnd);
    }

    private static String formatSendAck(long timestamp, int packetId) {
        return String.format(SND_ACK_FMT, timestamp, packetId);
    }

    private static String formatDiscardPacket(long timestamp, int packetId, char contents) {
        return String.format(DISCARD_PKT_FMT, timestamp, packetId, contents);
    }

    private static final String SND_PKT_FMT = "[%s] packet-%d %c sent";
    private static final String RCV_ACK_1_FMT = "[%s] ACK-%d recieved";
    private static final String RCV_ACK_2_FMT = "[%s] ACK-%d recieved; window = [%d, %d]";
    private static final String PKT_TIMEOUT_FMT = "[%s] packet-%d timeout";

    private static final String RCV_PKT_1_FMT = "[%s] packet-%d %c recieved";
    private static final String RCV_PKT_2_FMT = "[%s] packet-%d %c recieved; window = [%d, %d]";
    private static final String SND_ACK_FMT = "[%s] ACK-%d sent";
    private static final String DISCARD_PKT_FMT = "[%s] packet-%d %c discarded";

    public static void main(String[] args) throws NumberFormatException, SocketException {
        if (args.length != 5) {
            System.out.println("Usage: SRNode <src-port> <dst-port> <window-size> <time-out> <loss-rate>");
        }

        (new SRNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]),
                Integer.parseInt(args[3]), Float.parseFloat(args[4]))).run();
    }

}
