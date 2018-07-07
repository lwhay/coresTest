/**
 * 
 */
package countL;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.FilterBatchColumnReader;

/**
 * @author iclab
 *
 */
public class CountCOLFull {

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file);
        reader.createSchema(readSchema);
        reader.createRead(max);
        long count = 0;
        long tc = 0;
        long end = System.currentTimeMillis();
        System.out.println("time: " + (end - start));
        start = System.currentTimeMillis();
        while (reader.hasNext()) {
            Record r = reader.next();
            String address = r.get(2).toString();
            List<Record> orders = (List<Record>) r.get(8);
            for (Record order : orders) {
                String date = order.get(3).toString();
                List<Record> lines = (List<Record>) order.get(9);
                tc += lines.size();
                for (Record line : lines) {
                    count++;
                }
            }
        }
        reader.close();
        end = System.currentTimeMillis();
        System.out.println(count + ":" + tc);
        System.out.println("time: " + (end - start));

    }

}
