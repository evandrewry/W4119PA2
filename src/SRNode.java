import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
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
        private long start;
        private long end;
        
    	public AckPacket(long id, int src, int dst, long start, long end) {
    		this.id = id;
    		this.src = src;
    		this.dst = dst;
    		this.start = start;
    		this.end = end;
    	}
    	
    	public AckPacket(String payload) {
    		Matcher m = ACK_PAYLOAD_PATTERN.matcher(payload);
    		if (m.matches()) {
    			this.id = Long.parseLong(m.group(1));
    			this.src = Integer.parseInt(m.group(2));
    			this.dst = Integer.parseInt(m.group(3));
    			this.start = Long.parseLong(m.group(4));
    			this.end = Long.parseLong(m.group(5));
    		} else {
    			throw new RuntimeException("mismatch");
    		}
    	}

		public byte[] getPayload() {
			return String.format(ACK_PAYLOAD_FORMAT, id, src, dst, start, end).getBytes();
		}

		public boolean isWindowUpdate() {
			return start != 0 && end != 0;
		}
    }

	private InputReader inputReader;
    private UDPReciever reciever;
    private UDPSender sender;
    private DatagramSocket socket;
    private final ExecutorService pool;
    private int srcPort;
    private int dstPort;
    private int windowSize;
    private long senderWindowPosition = 0L;
    private boolean[] senderWindow;
    private Timer timer = new Timer();
    private long receiverWindowPosition = 0L;
    private boolean[] receiverWindow;
    private int timeout;
    private float lossRate;
    private long currentPacketId = 0L;
	private ArrayList<CharPacket> sendBuffer = new ArrayList<SRNode.CharPacket>();
    
    public SRNode(int srcPort, int dstPort, int windowSize, int timeout, float lossRate) throws SocketException {
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
        this.sender = new UDPSender(new DatagramSocket(), "localhost", srcPort, this);
        this.inputReader = new InputReader(this);
    }

    public void respond(DatagramPacket packet) {
    	if (Math.random() < this.lossRate) return;
        String data = new String(packet.getData(), 0, packet.getLength());
        if(PAYLOAD_PATTERN.matcher(data).matches()) {
        	CharPacket rcv = new CharPacket(data);
        	if (rcv.id >= receiverWindowPosition + windowSize || rcv.id < receiverWindowPosition) {
        		System.out.println(formatDiscardPacket(Calendar.getInstance().getTimeInMillis(), rcv.id, rcv.data));
        	} else {
        		receiverWindow[(int) (rcv.id % windowSize)] = true;
        		if (rcv.id == this.receiverWindowPosition) {
        			advanceRecieverWindow();
        			send(new AckPacket(rcv.id, rcv.dst, rcv.src, receiverWindowPosition, receiverWindowPosition + windowSize));
        			System.out.println(formatRecievePacket2(Calendar.getInstance().getTimeInMillis(), rcv.id, rcv.data, receiverWindowPosition, receiverWindowPosition + windowSize));
        		} else if (rcv.id > this.receiverWindowPosition && rcv.id < this.receiverWindowPosition + this.windowSize) {
        			send(new AckPacket(rcv.id, rcv.dst, rcv.src, 0, 0));
        			System.out.println(formatRecievePacket1(Calendar.getInstance().getTimeInMillis(), rcv.id, rcv.data));
        		}
        	}
        } else if (ACK_PAYLOAD_PATTERN.matcher(data).matches()) {
        	AckPacket ack = new AckPacket(data);
        	if (ack.id >= senderWindowPosition + windowSize || ack.id < senderWindowPosition) {
        		System.out.println("Stray ACK");
        	} else {
        		senderWindow[(int) (ack.id % windowSize)] = true;

            	if (ack.id == this.senderWindowPosition) {
            		advanceSenderWindow();
            	}

            	if (ack.isWindowUpdate()) {
            		System.out.println(formatRecieveAck2(Calendar.getInstance().getTimeInMillis(), ack.id, ack.start, ack.end));
            	} else {
            		System.out.println(formatRecieveAck1(Calendar.getInstance().getTimeInMillis(), ack.id));
            	}
        	}
        }
    }

    private void advanceRecieverWindow() {
    	int index = (int) (receiverWindowPosition % windowSize);
		while (index < windowSize && receiverWindow[index]) {
			receiverWindow[index] = false;
			receiverWindowPosition++;
			index++;
		}
	}

	private void advanceSenderWindow() {
    	int index = (int) (senderWindowPosition % windowSize);
		while (index < windowSize && senderWindow[index]) {
			senderWindow[index] = false;
			senderWindowPosition++;
			index++;
		}
		while (!sendBuffer.isEmpty() && sendBuffer.get(0).id < senderWindowPosition + windowSize) {
			send(sendBuffer.remove(0));
		}
	}

	private long getNextPacketId() {
		return currentPacketId++;
	}

	class AckTimerTask extends TimerTask  {
	    private CharPacket p;
		private SRNode handler;

	     public AckTimerTask(CharPacket p, SRNode handler) {
	         this.p = p;
	         this.handler = handler;
	     }

			@Override
			public void run() {
				if (p.id < (handler.senderWindowPosition + handler.windowSize) && p.id >= handler.senderWindowPosition && !handler.senderWindow[(int) (p.id % handler.windowSize)]) {
					send(p);
				}
			}
	}
	
	public void send(String s) {
		for (int i = 0; i < s.length(); i++) {
			long id = getNextPacketId();
			CharPacket p = new CharPacket(id, srcPort, dstPort, s.charAt(i));
			if (id < senderWindowPosition + windowSize) {
				send(p);
				timer.schedule(new AckTimerTask(p, this), this.timeout);
			} else {
				sendBuffer.add(p);
			}
		}
    }

	public void send(Packet packet) {
        this.sender.send(packet);
    }
	
    private void run() throws SocketException {
        send(); recieve(); read();
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
        private final ArrayList<Packet> packetBuffer = new ArrayList<Packet>();

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

        public void send(Packet packet) {
            synchronized (packetBuffer) {
                packetBuffer.add(packet);
                packetBuffer.notify();
            }
        }

        @Override
        public void run() {
            // Begin to send

            byte[] buffer = new byte[1];
            Packet packet;
            while (true) {
            	
                /* check if we have data to send */
                try {
                    synchronized (packetBuffer) {
                        if (packetBuffer.isEmpty()) packetBuffer.wait();
                        packet = packetBuffer.remove(0);
                    }
                } catch (InterruptedException e) {
                    continue;
                }

                	buffer = packet.getPayload();
                	
                	/* create packet to send */
                    DatagramPacket sendPacket;
                    try {
                        sendPacket = new DatagramPacket(buffer, buffer.length, InetAddress.getByName(receiverIP),
                                receiverPort);
                    } catch (UnknownHostException e) {
                        socket.close();
                        e.printStackTrace();
                        break;
                    }

                    /* send the packet */
                  //  boolean acked = false;
//                    do {
                        /* try to send */
                        try {
                            socket.send(sendPacket);
                            if (packet instanceof CharPacket) {
                            	CharPacket p = (CharPacket) packet;
                            	System.out.println(formatSendPacket(Calendar.getInstance().getTimeInMillis(), p.id, p.data));
                            } else if (packet instanceof AckPacket) {
                            	AckPacket p = (AckPacket) packet;
                                System.out.println(formatSendAck(Calendar.getInstance().getTimeInMillis(), p.id));
                            }
//    						acked = packet instanceof AckPacket || receiveAck();
                        } catch (IOException e) {
                            socket.close();
                            e.printStackTrace();
                            return;
                        }
                        
//                    } //while (!acked);
                    
                    /*
                     * System.out.println("[" + Calendar.getInstance().getTimeInMillis() + "] Sent to server: (IP: " +
                     * receiverIP + ", Port: " + String.valueOf(receiverPort) + "): " + inputString);
                     */
            }
        }

		private byte[] getPayload(char c) {
			byte[] payload = new byte[BUFFER_SIZE];
			payload[0] = (byte) c;
			return payload;
		}

		private boolean receiveAck() throws IOException {
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
			    return true;
			} catch (SocketTimeoutException e) {
			    //no ack was received, need to try again.
			    System.out.println("no ack received! resending packet......");
			    return false;
			}
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
                handler.handleInput(s);
            }
        }
    }

    private static String formatSendPacket(long timestamp, long packetId, char contents) {
        return String.format(SND_PKT_FMT, timestamp, packetId, contents);
    }

    public void handleInput(String s) {
		Matcher m = SND_CMD_PATTERN.matcher(s);
		if (m.matches()) {
			send(m.group(1));
		}
		
	}

    private static String formatPayload(long id, int src, int dst, byte[] data) {
    	return String.format(PAYLOAD_FORMAT, id, src, dst, data);
    }
    
	private static String formatRecieveAck1(long timestamp, long id) {
        return String.format(RCV_ACK_1_FMT, timestamp, id);
    }

    private static String formatRecieveAck2(long timestamp, long id, long start, long end) {
        return String.format(RCV_ACK_2_FMT, timestamp, id, start, end);
    }

    private static String formatPacketTimeout(long timestamp, int packetId) {
        return String.format(PKT_TIMEOUT_FMT, timestamp, packetId);
    }

    private static String formatRecievePacket1(long timestamp, long id, char contents) {
        return String.format(RCV_PKT_1_FMT, timestamp, id, contents);
    }

    private static String formatRecievePacket2(long timestamp, long id, char contents, long window2, long l) {
        return String.format(RCV_PKT_2_FMT, timestamp, id, contents, window2, l);
    }

    private static String formatSendAck(long timestamp, long id) {
        return String.format(SND_ACK_FMT, timestamp, id);
    }

    private static String formatDiscardPacket(long timestamp, long id, char contents) {
        return String.format(DISCARD_PKT_FMT, timestamp, id, contents);
    }

    private static final String SND_PKT_FMT = "[%s] packet-%d %c sent";
    private static final String RCV_ACK_1_FMT = "[%s] ACK-%d recieved";
    private static final String RCV_ACK_2_FMT = "[%s] ACK-%d recieved; window = [%d, %d]";
    private static final String PKT_TIMEOUT_FMT = "[%s] packet-%d timeout";

    private static final String RCV_PKT_1_FMT = "[%s] packet-%d %c recieved";
    private static final String RCV_PKT_2_FMT = "[%s] packet-%d %c recieved; window = [%d, %d]";
    private static final String SND_ACK_FMT = "[%s] ACK-%d sent";
    private static final String DISCARD_PKT_FMT = "[%s] packet-%d %c discarded";
    
    private static final Pattern SND_CMD_PATTERN = Pattern.compile("^send (.*)$", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern PAYLOAD_PATTERN = Pattern.compile("^id:(\\d*);src:(\\d*);dst:(\\d*);data:(.);$");
    private static final String PAYLOAD_FORMAT = "id:%d;src:%d;dst:%d;data:%s;";    
    private static final Pattern ACK_PAYLOAD_PATTERN = Pattern.compile("^ack:(\\d*);src:(\\d*);dst:(\\d*);start:(\\d*);end:(\\d*);$");
    private static final String ACK_PAYLOAD_FORMAT = "ack:%d;src:%d;dst:%d;start:%d;end:%d;";
    
    

    public static void main(String[] args) throws NumberFormatException, SocketException {
        if (args.length != 5) {
            System.out.println("Usage: SRNode <src-port> <dst-port> <window-size> <time-out> <loss-rate>");
            return;
        }

        (new SRNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]),
                Integer.parseInt(args[3]), Float.parseFloat(args[4]))).run();
    }

}
