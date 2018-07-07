/**
 * 
 */
package countL;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

/**
 * @author iclab
 *
 */
public class AvroCOL {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        long start = System.currentTimeMillis();
        DatumReader<Record> reader = new GenericDatumReader<Record>(readSchema);
        DataFileReader<Record> fileReader = new DataFileReader<Record>(file, reader);
        long count = 0;
        long tc = 0;
        long end = System.currentTimeMillis();
        System.out.println("time: " + (end - start));
        start = System.currentTimeMillis();
        while (fileReader.hasNext()) {
            Record r = fileReader.next();
            String address = r.get(0).toString();
            List<Record> orders = (List<Record>) r.get(1);
            for (Record order : orders) {
                String date = order.get(0).toString();
                List<Record> lines = (List<Record>) order.get(1);
                tc += lines.size();
                for (Record line : lines) {
                    count++;
                }
            }
        }
        fileReader.close();
        end = System.currentTimeMillis();
        System.out.println(count + ":" + tc);
        System.out.println("time: " + (end - start));
    }

}
