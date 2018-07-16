/**
 * 
 */
package compositionType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class doubleUnionTest {
    static final Map<String, String> part = new HashMap<>();
    static final Map<String, List<String>> nest = new HashMap<>();

    public static void prepare(String[] args) throws IOException {
        BufferedReader brp = new BufferedReader(new FileReader(args[2]));
        String line = "";
        while ((line = brp.readLine()) != null) {
            part.put(line.split("\\|")[0], line);
        }
        brp.close();
        BufferedReader brl = new BufferedReader(new FileReader(args[3]));
        while ((line = brl.readLine()) != null) {
            String key = line.split("\\|")[1];
            if (!nest.containsKey(key)) {
                nest.put(key, new ArrayList<>());
            }
            nest.get(key).add(line);
        }
        brl.close();
    }

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
        int count = 0;
        int tcount = 0;
        Random rand = new Random();
        for (Map.Entry<String, String> partpair : part.entrySet()) {
            Record record = new Record(s);
            String kpart = partpair.getKey();
            String[] fpart = partpair.getValue().split("\\|");
            for (int i = 0; i < s.getFields().size() - 1; i++) {
                record.put(i, fpart[i]);
            }
            List<String> lpart = nest.get(kpart);
            List<Record> lsr = new ArrayList<>();
            for (String l : lpart) {
                Schema inlineSchema = new Schema.Parser().parse(new File("./src/resources/union/lineitemnull.avsc"));
                Record rl = new Record(inlineSchema);
                String[] ls = l.split("\\|");
                for (int j = 0; j < inlineSchema.getFields().size(); j++) {
                    if (rand.nextDouble() > 0.5) {
                        rl.put(j, ls[j]);
                    }
                }
                tcount++;
                lsr.add(rl);
            }
            record.put(s.getFields().size() - 1, lsr);
            count++;
            //System.out.println(count++ + ":\t" + record);
            writer.flush(record);
            //writer.append(record);
        }
        System.out.println("higher: " + count + " lower: " + tcount);
        writer.flush();
        writer = new BatchAvroColumnWriter<Record>(s, args[1], 1000, 2000, "snappy");
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
    }

    public static void scanReader(String[] args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        BatchColumnReader<Record> bacr = new BatchColumnReader<Record>(new File(args[1] + "result.neci"));
        bacr.createSchema(s);
        bacr.create();
        int count = 0;
        //System.out.println(bacr.getRowCount(0));
        while (bacr.hasNext()) {
            count++;
            if (count % 256 == 0) {
                System.out.print("fr" + count + ":\t");
            }
            Record record = bacr.next();
            for (int i = 0; i < s.getFields().size(); i++) {
                Object obj = record.get(i);
                if (count % 256 == 0) {
                    if (obj != null) {
                        System.out.print(obj.toString() + "|");
                    } else {
                        System.out.print("null,");
                    }
                }
            }
            if (count % 256 == 0) {
                System.out.println();
            }
        }
        bacr.close();
    }

    public static void scanFilterReader(String[] args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        FilterBatchColumnReader<Record> bacr = new FilterBatchColumnReader<Record>(new File(args[1] + "result.neci"));
        bacr.createSchema(s);
        bacr.createRead(100);
        int count = 0;
        System.out.println(bacr.getRowCount(0));
        while (bacr.hasNext()) {
            count++;
            if (count % 256 == 0) {
                System.out.print("fr" + count + ":\t");
            }
            Record record = bacr.next();
            for (int i = 0; i < s.getFields().size(); i++) {
                Object obj = record.get(i);
                if (count % 256 == 0) {
                    if (obj != null) {
                        System.out.print(obj.toString() + "|");
                    } else {
                        System.out.print("null,");
                    }
                }
            }
            if (count % 256 == 0) {
                System.out.println();
            }
        }
        bacr.close();
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        prepare(args);
        build(args);
        scanReader(args);
        scanFilterReader(args);
    }
}
