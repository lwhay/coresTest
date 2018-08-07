/**
 * 
 */
package utils.AsyncQueue;

/**
 * @author Michael
 *
 */
public class SyncQueueProcess {
    private static int buflen;

    private static byte[] target;

    /**
     * @param args
     */
    public static void main(String[] args) {
        buflen = Integer.parseInt(args[0]);
        for (int i = 0; i < Integer.parseInt(args[1]) * Integer.parseInt(args[2]); i++) {
            target = new byte[buflen];
        }
    }

}
