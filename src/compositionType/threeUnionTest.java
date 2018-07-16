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
import java.util.Map.Entry;
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
public class threeUnionTest {
    static final Map<String, String> part = new HashMap<>();
    static final Map<String, Map<String, String>> middle = new HashMap<>();
    static final Map<String, List<String>> nest = new HashMap<>();

    public static void prepare(String[] args) throws IOException {
        BufferedReader brp = new BufferedReader(new FileReader(args[2]));
        String line = "";
        while ((line = brp.readLine()) != null) {
            String[] fields = line.split("\\|");
            part.put(fields[0].trim(), line);
        }
        brp.close();
        BufferedReader brps = new BufferedReader(new FileReader(args[3]));
        int pscount = 0;
        while ((line = brps.readLine()) != null) {
            String[] fields = line.split("\\|");
            String key = fields[0].trim();
            if (!middle.containsKey(key)) {
                middle.put(key, new HashMap<>());
            }
            if (middle.get(key).containsKey(fields[0].trim() + "|" + fields[1].trim())) {
                System.out.println(key + "<->" + fields[0].trim() + ":" + fields[1].trim());
            }
            middle.get(key).put(fields[0].trim() + "|" + fields[1].trim(), line);
            pscount++;
        }
        System.out.println(pscount);
        pscount = 0;
        for (Map<String, String> psrd : middle.values()) {
            pscount += psrd.size();
        }
        System.out.println(pscount);
        brps.close();
        BufferedReader brl = new BufferedReader(new FileReader(args[4]));
        int outer = 0;
        while ((line = brl.readLine()) != null) {
            String[] fields = line.split("\\|");
            String key = fields[1].trim() + "|" + fields[2].trim();
            if (!nest.containsKey(key)) {
                nest.put(key, new ArrayList<>());
            }
            if (!middle.get(fields[1].trim()).containsKey(key)) {
                System.out.println("outer: " + outer++);
            }
            nest.get(key).add(line);
        }
        brl.close();
    }

    public static void build(String[] args) throws IOException {
        Schema s = new Schema.Parser().parse(new File("./src/resources/union/nestpart.avsc"));
        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, args[1], 1000, 2000, "snappy");
        File[] oldfiles = new File(args[1]).listFiles();
        if (oldfiles != null) {
            System.out.println("Delete on " + args[1]);
            for (File file : oldfiles) {
                file.delete();
            }
        }
        int count = 0;
        Random rand = new Random();
        int nullinline = 0;
        int pscount = 0;
        for (Map<String, String> psrd : middle.values()) {
            pscount += psrd.size();
        }
        System.out.println(pscount);
        pscount = 0;
        for (Map.Entry<String, String> partpair : part.entrySet()) {
            Record precord = new Record(s);
            String kpart = partpair.getKey();
            String[] fpart = partpair.getValue().split("\\|");
            for (int i = 0; i < s.getFields().size() - 1; i++) {
                if (i == 0) {
                    precord.put(i, Integer.parseInt(fpart[i]));
                } else {
                    precord.put(i, fpart[i]);
                }
            }
            Map<String, String> mpart = middle.get(kpart);
            List<Record> pss = new ArrayList<>();
            for (Entry<String, String> ps : mpart.entrySet()) {
                Schema middleSchema = new Schema.Parser().parse(new File("./src/resources/union/nestpartsupp.avsc"));
                Record psrecord = new Record(middleSchema);
                String pskey = ps.getKey();
                String psstring = ps.getValue();
                String[] fps = psstring.split("\\|");
                if (rand.nextDouble() > 0.2) {
                    for (int j = 0; j < middleSchema.getFields().size() - 1; j++) {
                        if (j < 2) {
                            psrecord.put(j, Integer.parseInt(fps[j]));
                        } else {
                            psrecord.put(j, fps[j]);
                        }
                    }
                }
                Schema inlineSchema = new Schema.Parser().parse(new File("./src/resources/union/nestlineitem.avsc"));
                List<String> lines = nest.get(pskey);
                List<Record> lls = new ArrayList<>();
                if (lines == null) {
                    System.out.println(count);
                    nullinline++;
                } else {
                    for (String line : lines) {
                        Record lrecord = new Record(inlineSchema);
                        String[] lfields = line.split("\\|");
                        for (int k = 0; k < inlineSchema.getFields().size(); k++) {
                            if (k < 3) {
                                lrecord.put(k, Integer.parseInt(lfields[k]));
                            } else {
                                lrecord.put(k, lfields[k]);
                            }
                        }
                        lls.add(lrecord);
                    }
                }
                psrecord.put(middleSchema.getFields().size() - 1, lls);
                pss.add(psrecord);
            }
            precord.put(s.getFields().size() - 1, pss);
            pscount += pss.size();
            System.out.println(count++ + ":\t" + precord);
            writer.flush(precord);
        }
        System.out.println(nullinline + "<->" + pscount);
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
    }

    public static void scanReader(String[] args) throws IOException {
        {
            Schema s = new Schema.Parser().parse(new File("./src/resources/union/nestpart.avsc"));
            BatchColumnReader<Record> bacr = new BatchColumnReader<Record>(new File(args[1] + "result.neci"));
            /*BatchColumnReader<Record> bacr =
                    new BatchColumnReader<Record>(new File("F:\\coresRun/codec/tpch/ppsl/result/file0.neci"));*/
            bacr.createSchema(s);
            bacr.create();
            int count = 0;
            while (bacr.hasNext()) {
                Record record = bacr.next();
                String pk = record.get(0).toString();
                //System.out.println(pk);
                count++;
            }
            bacr.close();
            System.out.println("part: " + count);
        }
        {
            Schema s = new Schema.Parser().parse(new File("./src/resources/union/nestpartsupp.avsc"));
            BatchColumnReader<Record> bacr = new BatchColumnReader<Record>(new File(args[1] + "result.neci"));
            bacr.createSchema(s);
            bacr.create();
            int count = 0;
            int tc = bacr.getRowCount(12);
            while (bacr.hasNext()) {
                Record record = bacr.next();
                if (record.get(0) != null && record.get(1) != null && count % 256 == 0) {
                    String pk = record.get(0).toString();
                    String sk = record.get(1).toString();
                    System.out.print(count + ":" + pk + ":" + sk + "\t");
                    System.out.println(record.toString());
                }
                count++;
            }
            bacr.close();
            System.out.println("partsupp: " + count + " expect: " + tc);
        }
        {
            Schema s = new Schema.Parser().parse(new File("./src/resources/union/nestlineitem.avsc"));
            BatchColumnReader<Record> bacr = new BatchColumnReader<Record>(new File(args[1] + "result.neci"));
            bacr.createSchema(s);
            bacr.create();
            int count = 0;
            int tc = bacr.getRowCount(16);
            while (bacr.hasNext()) {
                Record record = bacr.next();
                if (record.get(0) != null && record.get(1) != null && count % 256 == 0) {
                    String pk = record.get(0).toString();
                    String sk = record.get(1).toString();
                    System.out.print(count + ":" + pk + ":" + sk + "\t");
                    System.out.println(record.toString());
                }
                count++;
            }
            bacr.close();
            System.out.println("partsupp: " + count + " expect: " + tc);
        }
    }

    public static void scanFilterReader(String[] args) throws IOException {
        {
            Schema s = new Schema.Parser().parse(new File("./src/resources/union/nestpart.avsc"));
            FilterBatchColumnReader<Record> bacr =
                    new FilterBatchColumnReader<Record>(new File(args[1] + "result.neci"));
            bacr.createSchema(s);
            bacr.createRead(257);
            int count = 0;
            while (bacr.hasNext()) {
                Record record = bacr.next();
                String pk = record.get(0).toString();
                count++;
            }
            bacr.close();
            System.out.println("part: " + count);
        }
        {
            Schema s = new Schema.Parser().parse(new File("./src/resources/union/nestpartsupp.avsc"));
            FilterBatchColumnReader<Record> bacr =
                    new FilterBatchColumnReader<Record>(new File(args[1] + "result.neci"));
            bacr.createSchema(s);
            bacr.createRead(257);
            int count = 0;
            int tc = bacr.getRowCount(1);
            while (bacr.hasNext()) {
                Record record = bacr.next();
                if (record.get(0) != null && record.get(1) != null && count % 256 == 0) {
                    String pk = record.get(0).toString();
                    String sk = record.get(1).toString();
                    System.out.print(count + ":" + pk + ":" + sk + "\t");
                    System.out.println(record.toString());
                }
                count++;
            }
            bacr.close();
            System.out.println("partsupp: " + count + " expect: " + tc);
        }
        {
            Schema s = new Schema.Parser().parse(new File("./src/resources/union/nestlineitem.avsc"));
            FilterBatchColumnReader<Record> bacr =
                    new FilterBatchColumnReader<Record>(new File(args[1] + "result.neci"));
            bacr.createSchema(s);
            bacr.createRead(257);
            int count = 0;
            int tc = bacr.getRowCount(1);
            while (bacr.hasNext()) {
                Record record = bacr.next();
                if (record.get(0) != null && record.get(1) != null && count % 256 == 0) {
                    String pk = record.get(0).toString();
                    String sk = record.get(1).toString();
                    System.out.print(count + ":" + pk + ":" + sk + "\t");
                    System.out.println(record.toString());
                }
                count++;
            }
            bacr.close();
            System.out.println("partsupp: " + count + " expect: " + tc);
        }
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
