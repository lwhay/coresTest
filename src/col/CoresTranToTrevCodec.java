/**
 * 
 */
package col;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnWriter;

import cores.avro.FilterBatchColumnReader;

/**
 * @author iclab
 *
 */
public class CoresTranToTrevCodec {
    public static void main(String[] args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        File fromFile = new File(args[1]);
        File toFile1 = new File(args[2]);
        int max = Integer.parseInt(args[4]);

        AvroColumnWriter<Record> writer1 = new AvroColumnWriter<Record>(s, new ColumnFileMetaData().setCodec(args[5]));
        if (!toFile1.getParentFile().exists()) {
            toFile1.getParentFile().mkdirs();
        }

        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(fromFile);
        reader.createSchema(s);
        //        long t1 = System.currentTimeMillis();
        //        reader.filterNoCasc();
        //        long t2 = System.currentTimeMillis();
        reader.createRead(max);

        while (reader.hasNext()) {
            Record r = reader.next();
            writer1.write(r);
        }
        reader.close();
        writer1.writeTo(toFile1);
    }
}
