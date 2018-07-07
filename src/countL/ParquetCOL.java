/**
 * 
 */
package countL;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetReader;

/**
 * @author iclab
 *
 */
public class ParquetCOL {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<GenericRecord>();
        readSupport.setRequestedProjection(conf, readSchema);
        readSupport.setAvroReadSchema(conf, readSchema);
        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> reader = new ParquetReader(conf, new Path(args[0]), readSupport);
        int count = 0;
        int tc = 0;
        long end = System.currentTimeMillis();
        System.out.println("time: " + (end - start));
        start = System.currentTimeMillis();
        GenericRecord r = reader.read();
        while (r != null) {
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
            r = reader.read();
        }
        reader.close();
        end = System.currentTimeMillis();
        System.out.println(count + ":" + tc);
        System.out.println("time: " + (end - start));
    }

}
