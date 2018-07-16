/**
 * 
 */
package compositionType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.BatchAvroColumnWriter;
import cores.avro.BatchColumnReader;
import cores.avro.FilterBatchColumnReader;

/**
 * @author Michael
 *
 */
public class singleUnionTest {
    public static void build(String[] args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, args[1], 1000, 2000, "snappy");
        File[] oldfiles = new File(args[1]).listFiles();
        if (oldfiles != null) {
            System.out.println("Delete on " + args[1]);
            for (File file : oldfiles) {
                file.delete();
            }
        }
        BufferedReader br = new BufferedReader(new FileReader(args[2]));
        String line = "";
        Random rand = new Random();
        while ((line = br.readLine()) != null) {
            Record record = new Record(s);
            String[] fields = line.split("\\|");
            for (int i = 0; i < s.getFields().size(); i++) {
                double randf = rand.nextDouble();
                //System.out.println(randf);
                if (i <= 3 || randf > 0.5) {
                    if (i == 0) {
                        record.put(i, Long.parseLong(fields[i]));
                    } else {
                        record.put(i, fields[i]);
                    }
                } else {
                    record.put(i, null);
                }
            }
            //System.out.println(record.toString());
            //writer.append(record);
            //writer.flush();
            writer.flush(record);
        }
        writer.flush();
        File[] files = new File(args[1]).listFiles();
        File[] toBeMerged = new File[files.length / 2];
        int fidx = 0;
        for (File file : files) {
            if (file.getAbsolutePath().endsWith("neci")) {
                toBeMerged[fidx++] = file;
            }
        }
        writer.mergeFiles(toBeMerged);
        writer.flush();
        br.close();
    }

    public static void scanReader(String[] args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        BatchColumnReader<Record> bacr = new BatchColumnReader<Record>(new File(args[1] + "result.neci"));
        bacr.createSchema(s);
        System.out.println(bacr.getRowCount(0));
        bacr.create();
        int count = 0;
        while (bacr.hasNext()) {
            System.out.print(count++ + ":\t");
            Record record = bacr.next();
            for (int i = 0; i < s.getFields().size(); i++) {
                Object obj = record.get(i);
                if (obj != null) {
                    System.out.print((String) obj + "|");
                } else {
                    System.out.print("null,");
                }
            }
            System.out.println();
        }
        bacr.close();
    }

    public static void scanFilterReader(String[] args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        FilterBatchColumnReader<Record> bacr = new FilterBatchColumnReader<Record>(new File(args[1] + "result.neci"));
        bacr.createSchema(s);
        System.out.println(bacr.getRowCount(0));
        //bacr.createRead(100000);
        bacr.createFilterRead();
        int count = 0;
        while (bacr.hasNext()) {
            System.out.print(count++ + ":\t");
            Record record = bacr.next();
            for (int i = 0; i < s.getFields().size(); i++) {
                Object obj = record.get(i);
                if (obj != null) {
                    System.out.print((String) obj + "|");
                } else {
                    System.out.print("null,");
                }
            }
            System.out.println();
        }
        bacr.close();
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        build(args);
        scanReader(args);
        scanFilterReader(args);
    }

}
