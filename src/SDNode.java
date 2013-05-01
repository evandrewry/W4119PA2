public class SDNode {

    public static void main(String[] args) {
        if (args.length < 1 || args.length % 2 != 1) {
            System.out.println("Usage: SDNode <port-number> <neighbor-1-port> <neighbor-1-loss-rate> .... <neighbor-n-port> <neighbor-n-loss-rate> [last]");
        }
    }
}
