package col;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import cores.avro.FilterBatchColumnReader;
import parquet.avro.AvroParquetWriter;

public class CoresTranToParquet {
    public static void main(String[] args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        File fromFile = new File(args[1]);
        File toFile = new File(args[2]);
        int max = Integer.parseInt(args[3]);
        AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(new Path(args[2]), s);
        if (!toFile.getParentFile().exists()) {
            toFile.getParentFile().mkdirs();
        }

        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(fromFile);
        reader.createSchema(s);
        //        long t1 = System.currentTimeMillis();
        //        reader.filterNoCasc();
        //        long t2 = System.currentTimeMillis();
        reader.createRead(max);

        while (reader.hasNext()) {
            Record r = reader.next();
            writer.write(r);
        }
        reader.close();
        writer.close();
    }
}
