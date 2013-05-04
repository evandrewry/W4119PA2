import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DVNode {

    private UDPReciever reciever;
    private UDPSender sender;
    private DatagramSocket socket;
    private final ExecutorService pool;
    private int port;
    private int[] neighbors;
    private float[] weights;
    private boolean last;
    private HashMap<Integer, RoutingTableEntry> routingTable = new HashMap<Integer, DVNode.RoutingTableEntry>();

    public DVNode(int port, int[] neighbors, float[] weights, boolean last)
	    throws SocketException {
	this.port = port;
	this.neighbors = neighbors;
	this.weights = weights;
	this.last = last;

	initRoutingTable();

	this.socket = new DatagramSocket(port);
	this.pool = Executors.newFixedThreadPool(2);
	this.reciever = new UDPReciever(socket, this);
	this.sender = new UDPSender(new DatagramSocket(), "localhost", this);

	if (this.last) {
	    broadcastRoutingTable();
	}

    }

    private void initRoutingTable() {
	routingTable.put(port, new RoutingTableEntry(port, 0));
	for (int i = 0; i < neighbors.length; i++) {
	    routingTable.put(neighbors[i], new RoutingTableEntry(neighbors[i],
		    weights[i]));
	}
	System.out.println(formatRoutingTable(timestamp(), port, routingTable));
    }

    public static void main(String[] args) {
	if (args.length <= 1) {
	    System.out.println(USAGE);
	    return;
	}

	try {
	    int port = Integer.parseInt(args[1]);
	    boolean last = args[args.length - 1].equals("last");
	    int n = (args.length - (last ? 3 : 2)) / 2;
	    int[] neighbors = new int[n];
	    float[] weights = new float[n];
	    for (int i = 0; i < n; i++) {
		neighbors[i] = Integer.parseInt(args[2 * i + 2]);
		weights[i] = Float.parseFloat(args[2 * i + 3]);
	    }
	    new DVNode(port, neighbors, weights, last).run();
	} catch (NumberFormatException e) {
	    System.out.println(USAGE);
	} catch (SocketException e) {
	    e.printStackTrace();
	}
    }

    public void respond(DatagramPacket packet) {
	String data = new String(packet.getData(), 0, packet.getLength());
	if (PAYLOAD_PATTERN.matcher(data).matches()) {
	    respondToDVPacket(new DVPacket(data));
	} else {
	    throw new RuntimeException("couldn't match payload to a known packet type");
	}
    }

    private void respondToDVPacket(DVPacket p) {
	System.out.println(formatReceiveMessage(timestamp(), p.dst, p.src));
	if (routingTable.containsKey(p.node)) {
	    RoutingTableEntry e = routingTable.get(p.node);
	    float newWeight = p.weight + routingTable.get(p.src).weight;
	    if (e.weight > newWeight) {
		e.weight = newWeight;
		e.next = p.src;
		System.out.println(formatRoutingTable(timestamp(), port,
			routingTable));
		broadcastRoutingTable();
	    }
	} else {
	    routingTable.put(p.node, new RoutingTableEntry(p.node, p.src,
		    p.weight + routingTable.get(p.src).weight));
	    System.out.println(formatRoutingTable(timestamp(), port,
		    routingTable));
	    broadcastRoutingTable();
	}
    }

    public void handleSend(Packet packet) {
	if (packet instanceof DVPacket) {
	    DVPacket p = (DVPacket) packet;
	    System.out.println(formatSendMessage(timestamp(), p.src, p.dst));
	}
    }

    private void run() throws SocketException {
	send();
	recieve();
    }

    private void recieve() throws SocketException {
	pool.execute(this.reciever);
    }

    private void send() throws SocketException {
	pool.execute(this.sender);
    }

    private void broadcastRoutingTable() {
	for (int node : routingTable.keySet()) {
	    broadcastEntry(routingTable.get(node));
	}

    }

    private void broadcastEntry(RoutingTableEntry e) {
	for (int neighbor : neighbors) {
	    send(new DVPacket(port, neighbor, e.node, e.next, e.weight));
	}
    }

    public void send(Packet packet) {
	this.sender.send(packet);
    }

    /**
     * Client UDP sender that takes input from console, converts it to a packet,
     * and sends it to the server.
     * 
     * @author evan
     * 
     */
    public class UDPSender implements Runnable {
	private final DatagramSocket socket;
	private static final int BUFFER_SIZE = 1024;
	private static final int ACK_TIMEOUT = 1000;
	private final String receiverIP;
	private final DVNode handler;
	private final ArrayList<Packet> packetBuffer = new ArrayList<Packet>();

	/**
	 * Send from specified client and specified socket to specified ip and
	 * port
	 * 
	 * @param socket
	 * @param recieverIP
	 * @param receiverPort
	 * @param dvNode
	 */
	public UDPSender(DatagramSocket socket, String recieverIP, DVNode dvNode) {
	    this.socket = socket;
	    this.receiverIP = recieverIP;
	    this.handler = dvNode;
	}

	public void send(Packet packet) {
	    synchronized (packetBuffer) {
		packetBuffer.add(packet);
		packetBuffer.notify();
	    }
	}

	private Packet pollPacketBuffer() throws InterruptedException {
	    Packet packet;
	    synchronized (packetBuffer) {
		if (packetBuffer.isEmpty())
		    packetBuffer.wait();
		packet = packetBuffer.remove(0);
	    }
	    return packet;
	}

	@Override
	public void run() {
	    // Begin to send

	    while (true) {
		/* check if we have data to send */
		Packet packet;
		try {
		    packet = pollPacketBuffer();
		} catch (InterruptedException e) {
		    continue;
		}

		byte[] buffer = packet.getPayload();

		/* create packet to send */
		DatagramPacket sendPacket;
		try {
		    sendPacket = new DatagramPacket(buffer, buffer.length,
			    InetAddress.getByName(receiverIP),
			    packet.getDestPort());
		} catch (UnknownHostException e) {
		    socket.close();
		    e.printStackTrace();
		    break;
		}

		try {
		    socket.send(sendPacket);
		    handler.handleSend(packet);
		} catch (IOException e) {
		    socket.close();
		    e.printStackTrace();
		    break;
		}
	    }
	}

    }

    /**
     * Client UDP reciever that receives packets from the server and notifies
     * the client
     * 
     * @author evan
     * 
     */
    public class UDPReciever implements Runnable {
	private final DatagramSocket socket;
	private final DVNode handler;
	private static final int BUFFER_SIZE = 1024;
	private static final String SUCCESS_MSG_FMT = "Receiving at port %d ...\n";

	/**
	 * Receive for specified client at specified socket.
	 * 
	 * @param socket
	 * @param handler
	 */
	public UDPReciever(DatagramSocket socket, DVNode handler) {
	    this.socket = socket;
	    this.handler = handler;
	}

	@Override
	public void run() {
	    System.out.printf(SUCCESS_MSG_FMT, socket.getLocalPort());

	    /* begin to receive UDP packets */
	    byte[] buffer = new byte[BUFFER_SIZE];
	    while (true) {
		DatagramPacket packet = new DatagramPacket(buffer,
			buffer.length);
		try {
		    socket.receive(packet);
		    handler.respond(packet);
		} catch (IOException e) {
		    e.printStackTrace();
		}
	    }
	}
    }

    private interface Packet {
	byte[] getPayload();

	int getDestPort();
    }

    private class DVPacket implements Packet {
	int src;
	int dst;
	int node;
	Integer next;
	float weight;

	public DVPacket(int src, int dst, int node, Integer next, float weight) {
	    this.src = src;
	    this.dst = dst;
	    this.node = node;
	    this.next = next;
	    this.weight = weight;
	}

	public DVPacket(String payload) {
	    Matcher m = PAYLOAD_PATTERN.matcher(payload);
	    if (m.matches()) {
		this.src = Integer.parseInt(m.group(1));
		this.dst = Integer.parseInt(m.group(2));
		this.node = Integer.parseInt(m.group(3));
		this.next = Integer.parseInt(m.group(4));
		this.weight = Float.parseFloat(m.group(5));
	    } else {
		throw new RuntimeException("mismatch");
	    }
	}

	public byte[] getPayload() {
	    return String.format(PAYLOAD_FMT, src, dst, node, next, weight)
		    .getBytes();
	}

	@Override
	public int getDestPort() {
	    return dst;
	}
    }

    private static String formatSendMessage(long timestamp, int from, int to) {
	return String.format(SND_MSG_FMT, timestamp, from, to);
    }

    private static String formatReceiveMessage(long timestamp, int at, int from) {
	return String.format(RCV_MSG_FMT, timestamp, at, from);
    }

    private static String formatRoutingTable(long timestamp, int port,
	    HashMap<Integer, RoutingTableEntry> routingTable) {
	return String.format(ROUTING_TABLE_FMT, timestamp, port,
		formatRoutingTableEntries(routingTable));
    }

    private static String formatRoutingTableEntries(
	    HashMap<Integer, RoutingTableEntry> routingTable) {
	String out = "";
	for (int node : routingTable.keySet()) {
	    RoutingTableEntry e = routingTable.get(node);
	    if (e.next == e.node) {
		out = String.format(ROUTING_TABLE_NEXT_HOP_ENTRY_FMT, e.node,
			e.weight, out);
	    } else {
		out = String.format(ROUTING_TABLE_ENTRY_FMT, e.node, e.next,
			e.weight, out);
	    }
	}
	return out;
    }

    private class RoutingTableEntry {
	int node;
	float weight;
	int next;

	public RoutingTableEntry(int node, float weight) {
	    this.node = node;
	    this.next = node;
	    this.weight = weight;
	}

	public RoutingTableEntry(int node, int next, float weight) {
	    this.node = node;
	    this.weight = weight;
	    this.next = next;
	}
    }

    private static long timestamp() {
	return Calendar.getInstance().getTimeInMillis();
    }

    private static final String SND_MSG_FMT = "[%s] Message sent from Node %d to Node %d";
    private static final String RCV_MSG_FMT = "[%s] Message received at Node %d from Node %d";

    private static final String PAYLOAD_FMT = "src:%d;dst:%d;node:%d;next:%d;weight:%s;";
    private static final Pattern PAYLOAD_PATTERN = Pattern
	    .compile("^src:(\\d+);dst:(\\d+);node:(\\d+);next:(\\d+);weight:(\\d*\\.?\\d*);$");

    private static final String ROUTING_TABLE_FMT = "[%s] Node %d - Routing Table\n%s";
    private static final String ROUTING_TABLE_ENTRY_FMT = "Node %d [next %d] -> (%s) \n%s";
    private static final String ROUTING_TABLE_NEXT_HOP_ENTRY_FMT = "Node %d -> (%s) \n%s";
    private static final String USAGE = "Usage: DVNode <port-number> <neighbor-1-port> <neighbor-1-weight> .... <neighbor-n-port> <neighbor-n-weight> [last]";

}
