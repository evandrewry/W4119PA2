
public class DVNode {
    public static void main(String[] args) {
        if (args.length < 1 || args.length % 2 != 1) {
            System.out.println("Usage: DVNode <port-number> <neighbor-1-port> <neighbor-1-weight> .... <neighbor-n-port> <neighbor-n-weight> [last]");
        }
    }
    
    private static String formatDiscardPacket(long timestamp, int packetId, char contents) {
        return String.format(DISCARD_PKT_FMT, timestamp, packetId, contents);
    }
    
    private static final String SND_MSG_FMT = "[%s] Message sent from Node %d to Node %d";
    private static final String RCV_MSG_FMT = "[%s] Message received at Node %d from Node %d"; 

}
