/**
 * 
 */
package col;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cores.avro.BatchAvroColumnWriter;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetReader;

/**
 * @author iclab
 *
 */
public class ParquetToCores {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        //Schema s = new Schema.Parser().parse(new File(args[1]));
        String resultPath = args[2];
        int free = Integer.parseInt(args[3]);
        int mul = Integer.parseInt(args[4]);
        int rowPerFile = Integer.parseInt(args[5]);
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<GenericRecord>();
        readSupport.setRequestedProjection(conf, readSchema);
        readSupport.setAvroReadSchema(conf, readSchema);
        int roundCount = 0;
        int fileCount = 0;
        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> reader = new ParquetReader(conf, new Path(args[0]), readSupport);
        BatchAvroColumnWriter<Record> writer =
                new BatchAvroColumnWriter<Record>(readSchema, resultPath + (fileCount) + "/", free, mul);
        int count = 0;
        int tc = 0;
        long end = System.currentTimeMillis();
        System.out.println("time: " + (end - start));
        start = System.currentTimeMillis();
        GenericRecord r = reader.read();
        while (r != null) {
            if (++roundCount % rowPerFile == 0) {
                writer.flush();

                File[] files = new File(resultPath + fileCount).listFiles();
                File[] mergeFiles = new File[files.length / 2];
                int fCount = 0;
                for (File neciFile : files) {
                    if (neciFile.getAbsolutePath().endsWith("neci")) {
                        mergeFiles[fCount++] = neciFile;
                    }
                }
                writer.mergeFiles(mergeFiles);
                System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@Flush " + fileCount
                        + " at offset: " + roundCount);
                writer = new BatchAvroColumnWriter<Record>(readSchema, resultPath + (++fileCount) + "/", free, mul);
            }
            writer.flush((Record) r);
            r = reader.read();
        }
        File[] files = new File(resultPath + fileCount).listFiles();
        File[] mergeFiles = new File[files.length / 2];
        int fCount = 0;
        for (File neciFile : files) {
            if (neciFile.getAbsolutePath().endsWith("neci")) {
                mergeFiles[fCount++] = neciFile;
            }
        }
        reader.close();
        writer.flush();
        end = System.currentTimeMillis();
        System.out.println(count + ":" + tc);
        System.out.println("time: " + (end - start));
    }

}
