import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SDNode {


    private static final String USAGE = "Usage: SDNode <port-number> <neighbor-1-port> <neighbor-1-loss-rate> .... <neighbor-n-port> <neighbor-n-loss-rate> [last]";
    private InputReader inputReader;
    private UDPReciever reciever;
    private UDPSender sender;
    private DatagramSocket socket;
    private final ExecutorService pool;
    private int port;
    private int[] neighbors;
    private HashMap<Integer, Float> lossRates = new HashMap<Integer, Float>();
    private boolean last;
    private HashMap<Integer, RoutingTableEntry> routingTable = new HashMap<Integer, RoutingTableEntry>();
    private static final int WINDOW_SIZE = 10;
    private HashMap<Integer, Long> sendBase = new HashMap<Integer, Long>();
    private HashMap<Integer, Boolean[]> senderWindow = new HashMap<Integer, Boolean[]>();
    private Timer timer = new Timer();
    private HashMap<Integer, Long> recieverBase = new HashMap<Integer, Long>();
    private HashMap<Integer, Boolean[]> receiverWindow = new HashMap<Integer, Boolean[]>();
    private static final int TIMEOUT = 300;
    private HashMap<Integer, Long> currentPacketId = new HashMap<Integer, Long>();
    private ArrayList<Packet> sendBuffer = new ArrayList<Packet>();

    public static void main(String[] args) {
	if (args.length < 1) {
	    System.out.println(USAGE);
	    return;
	}

	try {
	    int port = Integer.parseInt(args[0]);
	    boolean last = args[args.length - 1].equals("last");
	    int n = (args.length - (last ? 2 : 1)) / 2;
	    int[] neighbors = new int[n];
	    float[] lossRates = new float[n];
	    for (int i = 0; i < n; i++) {
		neighbors[i] = Integer.parseInt(args[2 * i + 1]);
		lossRates[i] = Float.parseFloat(args[2 * i + 2]);
	    }
	    new SDNode(port, neighbors, lossRates, last).run();
	} catch (NumberFormatException e) {
	    System.out.println(USAGE);
	} catch (SocketException e) {
	    e.printStackTrace();
	}
    }

    private Boolean[] emptyWindow() {
	Boolean[] window = new Boolean[WINDOW_SIZE];
	for (int i = 0; i < window.length; i++) {
	    window[i] = new Boolean(false);
	}
	return window;
    }

    public SDNode(int port, int[] neighbors, float[] lossRates, boolean last)
	    throws SocketException {
	this.port = port;
	this.neighbors = neighbors;
	this.last = last;

	for (int i = 0; i < neighbors.length; i++) {
	    int neighbor = neighbors[i];
	    this.lossRates.put(neighbor, lossRates[i]);
	    this.senderWindow.put(neighbor, emptyWindow());
	    this.receiverWindow.put(neighbor, emptyWindow());
	    this.sendBase.put(neighbor, new Long(0));
	    this.recieverBase.put(neighbor, new Long(0));
	    this.currentPacketId.put(neighbor, 0L);
	}

	initRoutingTable();

	this.socket = new DatagramSocket(port);
	this.pool = Executors.newFixedThreadPool(3);
	this.reciever = new UDPReciever(socket, this);
	this.sender = new UDPSender(new DatagramSocket(), "localhost", this);
	this.inputReader = new InputReader(this);

	if (this.last) {
	    broadcastRoutingTable();
	}

    }

    private void initRoutingTable() {
	routingTable.put(port, new RoutingTableEntry(port, 0));
	for (int i = 0; i < neighbors.length; i++) {
	    routingTable.put(neighbors[i], new RoutingTableEntry(neighbors[i],
		    1.f / (1.f - lossRates.get(neighbors[i]))));
	}
	System.out.println(formatRoutingTable(timestamp(), port, routingTable));
    }

    public void respond(DatagramPacket packet) {
	String data = new String(packet.getData(), 0, packet.getLength());
	if (ACK_PAYLOAD_PATTERN.matcher(data).matches()) {
	    respondToAck(new AckPacket(data));
	} else {
	    Packet rcv;
	    if (DV_PAYLOAD_PATTERN.matcher(data).matches()) {
		rcv = new DVPacket(data);
		respondToDVPacket((DVPacket) rcv);
	    } else {
		throw new RuntimeException(
			"couldn't match payload to a known packet type");
	    }
	}
    }



    private void sendAckAndMoveWindow(Packet rcv) {
	if (rcv.getId() >= recieverBase.get(rcv.getSrcPort()) + WINDOW_SIZE) {
	    /* discard packets outside the window */
	} else if (rcv.getId() < this.recieverBase.get(rcv.getSrcPort())
		|| receiverWindow.get(rcv.getSrcPort())[(int) (rcv.getId() % WINDOW_SIZE)]) {
	    /* send duplicate ack for already received packets */
	    send(new AckPacket(rcv.getId(), rcv.getDestPort(), rcv.getSrcPort()));
	    printReceivePacket1(rcv);
	} else {
	    /* we are receiving a packet in the window */
	    /* set the flag for this packet in the window */
	    receiverWindow.get(rcv.getSrcPort())[(int) (rcv.getId() % WINDOW_SIZE)] = true;
	    if (rcv.getId() == this.recieverBase.get(rcv.getSrcPort())) {
		/* if we are receiving the packet at the beginning of the */
		/* window, advance the window and send an ack */
		advanceRecieverWindow(rcv.getSrcPort());
		printReceivePacket2(rcv);
	    } else {
		printReceivePacket1(rcv);
	    }
	    send(new AckPacket(rcv.getId(), rcv.getDestPort(), rcv.getSrcPort()));
	}
    }

    private static String formatRecievePacket1(long timestamp, long id,
	    char contents) {
	return String.format(RCV_PKT_1_FMT, timestamp, id, contents);
    }

    private static String formatRecievePacket2(long timestamp, long id,
	    char contents, long window2, long l) {
	return String
		.format(RCV_PKT_2_FMT, timestamp, id, contents, window2, l);
    }

    private void printReceivePacket2(Packet rcv) {
	System.out.println(formatRecievePacket2(timestamp(), rcv.getId(), 'o',
		recieverBase.get(rcv.getSrcPort()),
		recieverBase.get(rcv.getSrcPort()) + WINDOW_SIZE));
    }

    private void printReceivePacket1(Packet rcv) {
	System.out.println(formatRecievePacket1(timestamp(), rcv.getId(), 'o'));
    }

    private void respondToAck(AckPacket ack) {
	if (ack.id >= sendBase.get(ack.src) + WINDOW_SIZE
		|| ack.id < sendBase.get(ack.src)) {
	    System.out.println("Stray ACK");
	} else {
	    senderWindow.get(ack.src)[(int) (ack.id % WINDOW_SIZE)] = true;

	    if (ack.id == this.sendBase.get(ack.src)) {
		advanceSenderWindow(ack.src);
		System.out.println(formatRecieveAck2(timestamp(), ack.id,
			sendBase.get(ack.src), sendBase.get(ack.src)
				+ WINDOW_SIZE));
	    } else {
		System.out.println(formatRecieveAck1(timestamp(), ack.id));
	    }
	}
    }

    private void advanceRecieverWindow(int port) {
	int index = (int) (recieverBase.get(port) % WINDOW_SIZE);
	while (receiverWindow.get(port)[index % WINDOW_SIZE]) {
	    receiverWindow.get(port)[index % WINDOW_SIZE] = false;
	    recieverBase.put(port, recieverBase.get(port) + 1);
	    index++;
	}
    }

    private void advanceSenderWindow(int port) {
	int index = (int) (sendBase.get(port) % WINDOW_SIZE);
	while (senderWindow.get(port)[index % WINDOW_SIZE]) {
	    senderWindow.get(port)[index % WINDOW_SIZE] = false;
	    sendBase.put(port, sendBase.get(port) + 1);
	    index++;
	}
	while (!sendBuffer.isEmpty()
		&& sendBuffer.get(0).getId() < sendBase.get(port) + WINDOW_SIZE) {
	    sendWithTimeout(sendBuffer.remove(0));
	}
    }

    private void sendWithTimeout(Packet p) {
	send(p);
	timer.schedule(new AckTimeout(p, this), TIMEOUT);
    }

    private class AckTimeout extends TimerTask {
	private Packet p;
	private SDNode handler;

	public AckTimeout(Packet p, SDNode handler) {
	    this.p = p;
	    this.handler = handler;
	}

	@Override
	public void run() {
	    if (shouldResend()) {
		sendWithTimeout(p);
		System.out.println(formatPacketTimeout(timestamp(), p.getId()));
	    }
	}

	private boolean shouldResend() {
	    return p.getId() < (handler.sendBase.get(p.getDestPort()) + WINDOW_SIZE)
		    && p.getId() >= handler.sendBase.get(p.getDestPort())
		    && !handler.senderWindow.get(p.getDestPort())[(int) (p
			    .getId() % WINDOW_SIZE)];
	}

    }

    private void respondToDVPacket(DVPacket p) {
	if (Math.random() < this.lossRates.get(p.getSrcPort())) {
	    //System.out.println(formatDiscardPacket(timestamp(), p.getId(), 'o'));
	    return;
	}

	sendAckAndMoveWindow(p);
	
	if (p.isLinkUpdate && p.node == this.port && p.weight != routingTable.get(p.src).weight) {
	    routingTable.put(p.src, new RoutingTableEntry(p.src, p.weight));
	    this.lossRates.put(p.src, 1f / p.weight + 1);
	    broadcastEntry(routingTable.get(p.src), true);
	}

	System.out.println(formatReceiveMessage(timestamp(), p.dst, p.src));

	if (routingTable.containsKey(p.node)) {
	    RoutingTableEntry e = routingTable.get(p.node);
	    float newWeight = p.weight + routingTable.get(p.src).weight;
	    if (p.isLinkUpdate || e.weight > newWeight) {
		e.weight = newWeight;
		e.next = p.src;
		System.out.println(formatRoutingTable(timestamp(), port,
			routingTable));
		broadcastEntry(routingTable.get(p.src), false);
	    }
	} else {
	    routingTable.put(p.node, new RoutingTableEntry(p.node, p.src,
		    p.weight + routingTable.get(p.src).weight));
	    System.out.println(formatRoutingTable(timestamp(), port,
		    routingTable));
	    broadcastEntry(routingTable.get(p.src), false);
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

    private class InputReader implements Runnable {
	private final SDNode handler;

	public InputReader(SDNode sdNode) {
	    this.handler = sdNode;
	}

	@Override
	public void run() {
	    BufferedReader input = new BufferedReader(new InputStreamReader(
		    System.in));
	    String s;

	    /* read input from console */
	    while (true) {
		try {
		    s = input.readLine();
		} catch (IOException e) {
		    socket.close();
		    e.printStackTrace();
		    continue;
		}
		handler.handleInput(s);
	    }
	}
    }

    public void handleInput(String s) {
	Matcher m = CHANGE_CMD_PATTERN.matcher(s);
	if (m.matches()) {
	    String args = m.group(1);
	    StringTokenizer st = new StringTokenizer(args);
	    while (st.hasMoreTokens()) {
		int port = Integer.parseInt(st.nextToken());
		if (!st.hasMoreTokens()) {
		    System.out.println(CHANGE_CMD_USAGE);
		    return;
		}
		float lossRate = Float.parseFloat(st.nextToken());

		if (Arrays.asList(neighbors).contains(port)) {
		    routingTable.put(port, new RoutingTableEntry(port,
			    1.0f / (1.0f - lossRate)));
		    lossRates.put(port, lossRate);
		    System.out.println(formatRoutingTableEntries(routingTable));
		}

		broadcastUpdateEntry(routingTable.get(port));
	    }
	}

    }

    private void broadcastRoutingTable() {
	for (int node : routingTable.keySet()) {
	    broadcastEntry(routingTable.get(node), false);
	}

    }

    private void broadcastEntry(RoutingTableEntry e, boolean forceUpdate) {
	for (int neighbor : neighbors) {
	    sendWithTimeout(new DVPacket(getNextPacketId(neighbor), port, neighbor,
		    e.node, e.next, e.weight, forceUpdate));
	}
    }

    private void broadcastUpdateEntry(RoutingTableEntry e) {
	for (int neighbor : neighbors) {
	    sendWithTimeout(new DVPacket(getNextPacketId(neighbor), port, neighbor,
		    e.node, e.next, e.weight, true));
	}
    }

    private long getNextPacketId(int neighbor) {
	long id = currentPacketId.get(neighbor);
	currentPacketId.put(neighbor, id + 1);
	return id + 1;
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
	private final String receiverIP;
	private final SDNode handler;
	private final ArrayList<Packet> packetBuffer = new ArrayList<Packet>();

	/**
	 * Send from specified client and specified socket to specified ip and
	 * port
	 * 
	 * @param socket
	 * @param recieverIP
	 * @param receiverPort
	 * @param node
	 */
	public UDPSender(DatagramSocket socket, String recieverIP, SDNode node) {
	    this.socket = socket;
	    this.receiverIP = recieverIP;
	    this.handler = node;
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
	private final SDNode handler;
	private static final int BUFFER_SIZE = 1024;
	private static final String SUCCESS_MSG_FMT = "Receiving at port %d ...\n";

	/**
	 * Receive for specified client at specified socket.
	 * 
	 * @param socket
	 * @param sdNode
	 */
	public UDPReciever(DatagramSocket socket, SDNode sdNode) {
	    this.socket = socket;
	    this.handler = sdNode;
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

	int getSrcPort();

	long getId();

	int getDestPort();
    }

    private class DVPacket implements Packet {
	long id;
	int src;
	int dst;
	int node;
	Integer next;
	float weight;
	boolean isLinkUpdate;

	public DVPacket(long id, int src, int dst, int node, Integer next,
		float weight, boolean isLinkUpdate) {
	    this.id = id;
	    this.src = src;
	    this.dst = dst;
	    this.node = node;
	    this.next = next;
	    this.weight = weight;
	    this.isLinkUpdate = isLinkUpdate;
	}

	public DVPacket(String payload) {
	    Matcher m = DV_PAYLOAD_PATTERN.matcher(payload);
	    if (m.matches()) {
		this.id = Integer.parseInt(m.group(1));
		this.src = Integer.parseInt(m.group(2));
		this.dst = Integer.parseInt(m.group(3));
		this.node = Integer.parseInt(m.group(4));
		this.next = Integer.parseInt(m.group(5));
		this.weight = Float.parseFloat(m.group(6));
		this.isLinkUpdate = Boolean.parseBoolean(m.group(7));
	    } else {
		throw new RuntimeException("mismatch");
	    }
	}

	public byte[] getPayload() {
	    return String.format(DV_PAYLOAD_FMT, id, src, dst, node, next,
		    weight, isLinkUpdate).getBytes();
	}

	@Override
	public int getDestPort() {
	    return dst;
	}

	@Override
	public long getId() {
	    return id;
	}

	@Override
	public int getSrcPort() {
	    return this.src;
	}
    }

    private class AckPacket implements Packet {
	private long id;
	private int src;
	private int dst;

	public AckPacket(long id, int src, int dst) {
	    this.id = id;
	    this.src = src;
	    this.dst = dst;
	}

	public AckPacket(String payload) {
	    Matcher m = ACK_PAYLOAD_PATTERN.matcher(payload);
	    if (m.matches()) {
		this.id = Long.parseLong(m.group(1));
		this.src = Integer.parseInt(m.group(2));
		this.dst = Integer.parseInt(m.group(3));
	    } else {
		throw new RuntimeException("mismatch");
	    }
	}

	public byte[] getPayload() {
	    return String.format(ACK_PAYLOAD_FORMAT, id, src, dst).getBytes();
	}

	@Override
	public int getDestPort() {
	    return this.dst;
	}

	@Override
	public long getId() {
	    return this.id;
	}

	@Override
	public int getSrcPort() {
	    return this.src;
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

    private static String formatRecieveAck1(long timestamp, long id) {
	return String.format(RCV_ACK_1_FMT, timestamp, id);
    }

    private static String formatRecieveAck2(long timestamp, long id,
	    long start, long end) {
	return String.format(RCV_ACK_2_FMT, timestamp, id, start, end);
    }

    private static String formatPacketTimeout(long timestamp, long id) {
	return String.format(PKT_TIMEOUT_FMT, timestamp, id);
    }

    private static final String PKT_TIMEOUT_FMT = "[%s] packet-%d timeout";

    private static final String SND_MSG_FMT = "[%s] Message sent from Node %d to Node %d";
    private static final String RCV_MSG_FMT = "[%s] Message received at Node %d from Node %d";

    private static final String DV_PAYLOAD_FMT = "id:%d;src:%d;dst:%d;node:%d;next:%d;weight:%s;update:%s";
    private static final Pattern DV_PAYLOAD_PATTERN = Pattern
	    .compile("^id:(\\d*);src:(\\d+);dst:(\\d+);node:(\\d+);next:(\\d+);weight:(\\d*\\.?\\d*);update:(true|false)$");

    private static final String ROUTING_TABLE_FMT = "[%s] Node %d - Routing Table\n%s";
    private static final String ROUTING_TABLE_ENTRY_FMT = "Node %d [next %d] -> (%s) \n%s";
    private static final String ROUTING_TABLE_NEXT_HOP_ENTRY_FMT = "Node %d -> (%s) \n%s";

    // private static final Pattern PAYLOAD_PATTERN =
    // Pattern.compile("^id:(\\d*);src:(\\d*);dst:(\\d*);data:(.);$");
    // private static final String PAYLOAD_FORMAT =
    // "id:%d;src:%d;dst:%d;data:%s;";
    private static final Pattern ACK_PAYLOAD_PATTERN = Pattern
	    .compile("^ack:(\\d*);src:(\\d*);dst:(\\d*);$");
    private static final String ACK_PAYLOAD_FORMAT = "ack:%d;src:%d;dst:%d;";
    private static final String RCV_ACK_1_FMT = "[%s] ACK-%d recieved";
    private static final String RCV_ACK_2_FMT = "[%s] ACK-%d recieved; window = [%d, %d]";

    private static final String SND_PKT_FMT = "[%s] packet-%d %c sent";
    private static final String RCV_PKT_1_FMT = "[%s] packet-%d %c recieved";
    private static final String RCV_PKT_2_FMT = "[%s] packet-%d %c recieved; window = [%d, %d]";
    private static final String SND_ACK_FMT = "[%s] ACK-%d sent";
    private static final Pattern SND_CMD_PATTERN = Pattern.compile(
	    "^send (.*)$", Pattern.CASE_INSENSITIVE);
    // private static final Pattern PAYLOAD_PATTERN =
    // Pattern.compile("^id:(\\d*);src:(\\d*);dst:(\\d*);data:(.);$");
    // private static final String PAYLOAD_FORMAT =
    // "id:%d;src:%d;dst:%d;data:%s;";

    private static final Pattern CHANGE_CMD_PATTERN = Pattern
	    .compile("change(( (\\d+) (\\d*\\.?\\d*))+)");
    private static final String CHANGE_CMD_USAGE = "change <node1port> <node1loss-rate>.... <nodeiport> <nodeiloss-rate>";

}
