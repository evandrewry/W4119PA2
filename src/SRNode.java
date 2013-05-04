import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SRNode {

    public static void main(String[] args) {
	if (args.length != 5) {
	    System.out.println(USAGE);
	    return;
	}

	try {
	    (new SRNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
		    Integer.parseInt(args[2]), Integer.parseInt(args[3]),
		    Float.parseFloat(args[4]))).run();
	} catch (NumberFormatException e) {
	    System.out.println(USAGE);
	} catch (SocketException e) {
	    e.printStackTrace();
	}
    }

    public SRNode(int srcPort, int dstPort, int windowSize, int timeout,
	    float lossRate) throws SocketException {
	this.srcPort = srcPort;
	this.dstPort = dstPort;
	this.windowSize = windowSize;
	this.senderWindow = new boolean[windowSize];
	this.receiverWindow = new boolean[windowSize];
	this.timeout = timeout;
	this.lossRate = lossRate;
	this.socket = new DatagramSocket(dstPort);
	this.pool = Executors.newFixedThreadPool(3);
	this.reciever = new UDPReciever(socket, this);
	this.sender = new UDPSender(new DatagramSocket(), "localhost", srcPort,
		this);
	this.inputReader = new InputReader(this);
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

    public void respond(DatagramPacket packet) {
	String data = new String(packet.getData(), 0, packet.getLength());
	if (PAYLOAD_PATTERN.matcher(data).matches()) {
	    respondToCharPacket(new CharPacket(data));
	} else if (ACK_PAYLOAD_PATTERN.matcher(data).matches()) {
	    respondToAck(new AckPacket(data));
	}
    }

    private void respondToAck(AckPacket ack) {
	if (ack.id >= sendBase + windowSize || ack.id < sendBase) {
	    System.out.println("Stray ACK");
	} else {
	    senderWindow[(int) (ack.id % windowSize)] = true;

	    if (ack.id == this.sendBase) {
		advanceSenderWindow();
		System.out.println(formatRecieveAck2(timestamp(), ack.id,
			sendBase, sendBase + windowSize));
	    } else {
		System.out.println(formatRecieveAck1(timestamp(), ack.id));
	    }
	}
    }

    private void respondToCharPacket(CharPacket rcv) {
	if (Math.random() < this.lossRate) {
	    System.out.println(formatDiscardPacket(timestamp(), rcv.id,
		    rcv.data));
	} else if (rcv.id >= recieverBase + windowSize) {
	    /* discard packets outside the window */
	} else if (rcv.id < this.recieverBase
		|| receiverWindow[(int) (rcv.id % windowSize)]) {
	    /* send duplicate ack for already received packets */
	    send(new AckPacket(rcv.id, rcv.dst, rcv.src));
	    printReceivePacket1(rcv);
	} else {
	    /* we are receiving a packet in the window */
	    /* set the flag for this packet in the window */
	    receiverWindow[(int) (rcv.id % windowSize)] = true;
	    if (rcv.id == this.recieverBase) {
		/* if we are receiving the packet at the beginning of the */
		/* window, advance the window and send an ack */
		advanceRecieverWindow();
		printReceivePacket2(rcv);
	    } else {
		printReceivePacket1(rcv);
	    }
	    send(new AckPacket(rcv.id, rcv.dst, rcv.src));
	}
    }

    private void advanceRecieverWindow() {
	int index = (int) (recieverBase % windowSize);
	while (receiverWindow[index % windowSize]) {
	    receiverWindow[index % windowSize] = false;
	    recieverBase++;
	    index++;
	}
    }

    private void advanceSenderWindow() {
	int index = (int) (sendBase % windowSize);
	while (senderWindow[index % windowSize]) {
	    senderWindow[index % windowSize] = false;
	    sendBase++;
	    index++;
	}
	while (!sendBuffer.isEmpty()
		&& sendBuffer.get(0).id < sendBase + windowSize) {
	    sendWithTimeout(sendBuffer.remove(0));
	}
    }

    private void sendWithTimeout(CharPacket p) {
	send(p);
	timer.schedule(new AckTimeout(p, this), this.timeout);
    }

    private long getNextPacketId() {
	return currentPacketId++;
    }

    private class AckTimeout extends TimerTask {
	private CharPacket p;
	private SRNode handler;

	public AckTimeout(CharPacket p, SRNode handler) {
	    this.p = p;
	    this.handler = handler;
	}

	@Override
	public void run() {
	    if (shouldResend()) {
		sendWithTimeout(p);
		System.out.println(formatPacketTimeout(timestamp(), p.id));
	    }
	}

	private boolean shouldResend() {
	    return p.id < (handler.sendBase + handler.windowSize)
		    && p.id >= handler.sendBase
		    && !handler.senderWindow[(int) (p.id % handler.windowSize)];
	}

    }

    public void send(String s) {
	for (int i = 0; i < s.length(); i++) {
	    long id = getNextPacketId();
	    CharPacket p = new CharPacket(id, srcPort, dstPort, s.charAt(i));
	    if (id < sendBase + windowSize) {
		sendWithTimeout(p);
	    } else {
		sendBuffer.add(p);
	    }
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
	private final int receiverPort;
	private final SRNode handler;
	private final ArrayList<Packet> packetBuffer = new ArrayList<Packet>();

	/**
	 * Send from specified client and specified socket to specified ip and
	 * port
	 * 
	 * @param socket
	 * @param recieverIP
	 * @param receiverPort
	 * @param handler
	 */
	public UDPSender(DatagramSocket socket, String recieverIP,
		int receiverPort, SRNode handler) {
	    this.socket = socket;
	    this.receiverIP = recieverIP;
	    this.receiverPort = receiverPort;
	    this.handler = handler;
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
			    InetAddress.getByName(receiverIP), receiverPort);
		} catch (UnknownHostException e) {
		    socket.close();
		    e.printStackTrace();
		    break;
		}

		try {
		    socket.send(sendPacket);
		    if (packet instanceof CharPacket) {
			CharPacket p = (CharPacket) packet;
			System.out.println(formatSendPacket(timestamp(), p.id,
				p.data));
		    } else if (packet instanceof AckPacket) {
			AckPacket p = (AckPacket) packet;
			System.out.println(formatSendAck(timestamp(), p.id));
		    }
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

    private class InputReader implements Runnable {
	private final SRNode handler;

	public InputReader(SRNode handler) {
	    this.handler = handler;
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
	Matcher m = SND_CMD_PATTERN.matcher(s);
	if (m.matches()) {
	    send(m.group(1));
	}

    }

    private interface Packet {
	byte[] getPayload();
    }

    private class CharPacket implements Packet {
	private long id;
	private int src;
	private int dst;
	private char data;

	public CharPacket(long id, int src, int dst, char c) {
	    this.id = id;
	    this.src = src;
	    this.dst = dst;
	    this.data = c;
	}

	public CharPacket(String payload) {
	    Matcher m = PAYLOAD_PATTERN.matcher(payload);
	    if (m.matches()) {
		this.id = Long.parseLong(m.group(1));
		this.src = Integer.parseInt(m.group(2));
		this.dst = Integer.parseInt(m.group(3));
		this.data = m.group(4).charAt(0);
	    } else {
		throw new RuntimeException("mismatch");
	    }
	}

	public byte[] getPayload() {
	    return String.format(PAYLOAD_FORMAT, id, src, dst, data).getBytes();
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
    }

    private void printReceivePacket2(CharPacket rcv) {
	System.out.println(formatRecievePacket2(timestamp(), rcv.id, rcv.data,
		recieverBase, recieverBase + windowSize));
    }

    private void printReceivePacket1(CharPacket rcv) {
	System.out.println(formatRecievePacket1(timestamp(), rcv.id, rcv.data));
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

    private static String formatRecievePacket1(long timestamp, long id,
	    char contents) {
	return String.format(RCV_PKT_1_FMT, timestamp, id, contents);
    }

    private static String formatRecievePacket2(long timestamp, long id,
	    char contents, long window2, long l) {
	return String
		.format(RCV_PKT_2_FMT, timestamp, id, contents, window2, l);
    }

    private static String formatSendAck(long timestamp, long id) {
	return String.format(SND_ACK_FMT, timestamp, id);
    }

    private static String formatDiscardPacket(long timestamp, long id,
	    char contents) {
	return String.format(DISCARD_PKT_FMT, timestamp, id, contents);
    }

    private static String formatSendPacket(long timestamp, long packetId,
	    char contents) {
	return String.format(SND_PKT_FMT, timestamp, packetId, contents);
    }

    private static long timestamp() {
	return Calendar.getInstance().getTimeInMillis();
    }

    private InputReader inputReader;
    private UDPReciever reciever;
    private UDPSender sender;
    private DatagramSocket socket;
    private final ExecutorService pool;
    private int srcPort;
    private int dstPort;
    private int windowSize;
    private long sendBase = 0L;
    private boolean[] senderWindow;
    private Timer timer = new Timer();
    private long recieverBase = 0L;
    private boolean[] receiverWindow;
    private int timeout;
    private float lossRate;
    private long currentPacketId = 0L;
    private ArrayList<CharPacket> sendBuffer = new ArrayList<CharPacket>();

    private static final String USAGE = "Usage: SRNode <src-port> <dst-port> <window-size> <time-out> <loss-rate>";
    private static final String SND_PKT_FMT = "[%s] packet-%d %c sent";
    private static final String RCV_ACK_1_FMT = "[%s] ACK-%d recieved";
    private static final String RCV_ACK_2_FMT = "[%s] ACK-%d recieved; window = [%d, %d]";
    private static final String PKT_TIMEOUT_FMT = "[%s] packet-%d timeout";
    private static final String RCV_PKT_1_FMT = "[%s] packet-%d %c recieved";
    private static final String RCV_PKT_2_FMT = "[%s] packet-%d %c recieved; window = [%d, %d]";
    private static final String SND_ACK_FMT = "[%s] ACK-%d sent";
    private static final String DISCARD_PKT_FMT = "[%s] packet-%d %c discarded";
    private static final Pattern SND_CMD_PATTERN = Pattern.compile(
	    "^send (.*)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PAYLOAD_PATTERN = Pattern
	    .compile("^id:(\\d*);src:(\\d*);dst:(\\d*);data:(.);$");
    private static final String PAYLOAD_FORMAT = "id:%d;src:%d;dst:%d;data:%s;";
    private static final Pattern ACK_PAYLOAD_PATTERN = Pattern
	    .compile("^ack:(\\d*);src:(\\d*);dst:(\\d*);$");
    private static final String ACK_PAYLOAD_FORMAT = "ack:%d;src:%d;dst:%d;";
}
