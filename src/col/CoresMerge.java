/**
 * 
 */
package col;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.BatchAvroColumnWriter;

/**
 * @author iclab
 *
 */
public class CoresMerge {

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
        String codec = args[5];
        long start = System.currentTimeMillis();
        BatchAvroColumnWriter<Record> writer =
                new BatchAvroColumnWriter<Record>(readSchema, resultPath, free, mul, codec);
        File[] files = file.listFiles();
        File[] mergeFiles = new File[files.length / 2];
        int fCount = 0;
        for (File neciFile : files) {
            if (neciFile.getAbsolutePath().endsWith("neci")) {
                mergeFiles[fCount++] = neciFile;
            }
        }
        writer.mergeFiles(mergeFiles);
        writer.flush();
        long end = System.currentTimeMillis();
        System.out.println("time: " + (end - start));

    }

}
