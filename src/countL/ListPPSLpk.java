/**
 * 
 */
package countL;

/**
 * @author lwh
 *
 */
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.FilterBatchColumnReader;

public class ListPPSLpk {

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
            long pk = (long) r.get(0);
            String mfgr = r.get(1).toString();
            System.out.println(pk + "\t" + mfgr);
        }
        reader.close();
        end = System.currentTimeMillis();
        System.out.println(count + ":" + tc);
        System.out.println("time: " + (end - start));

    }

}
