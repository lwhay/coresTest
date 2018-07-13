/**
 * 
 */
package tpch.single;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.trevni.TrevniRuntimeException;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;

import cores.avro.FilterBatchColumnReader;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetReader;

/**
 * @author Michael
 *
 */
public class Q01_Scan {
    public static void avroExecutor(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String t1 = args[2]; //l_shipdate
        long start = System.currentTimeMillis();
        DatumReader<Record> reader = new GenericDatumReader<Record>(readSchema);
        DataFileReader<Record> fileReader = new DataFileReader<Record>(file, reader);
        int count = 0;
        byte[] tmp = new byte[2];
        long selectivity = 0;
        Map<String, float[]> gbyMap = new HashMap<>();
        while (fileReader.hasNext()) {
            Record s = fileReader.next();
            List<Record> psa = (List<Record>) (s.get(8));
            for (Record ps : psa) {
                List<Record> la = (List<Record>) (ps.get(9));
                for (Record r : la) {
                    if (((String) r.get(10)).compareTo(t1) <= 0) {
                        float quantity = (float) r.get(4);
                        float extendedprice = (float) r.get(5);
                        float discount = (float) r.get(6);
                        float tax = (float) r.get(7);
                        byte[] returnflag = ((ByteBuffer) r.get(8)).array();
                        byte[] linestatus = ((ByteBuffer) r.get(9)).array();
                        if (returnflag.length != 1 || linestatus.length != 1) {
                            System.out.println("need to handle this." + " rf: " + returnflag[0] + " out of: "
                                    + returnflag.length + " ls: " + linestatus[0] + " out of: " + linestatus.length
                                    + " tk: " + selectivity);
                        } else {
                            //System.out.println(selectivity + " out of: " + reader.getRowCount(0));
                        }
                        tmp[0] = returnflag[0];
                        tmp[1] = linestatus[0];
                        String bb = new String(tmp);
                        if (!gbyMap.containsKey(bb)) {
                            byte[] key = new byte[2];
                            key[0] = tmp[0];
                            key[1] = tmp[1];
                            gbyMap.put(new String(key), new float[8]);
                        }
                        float[] values = gbyMap.get(bb);
                        values[0] += quantity;
                        values[1] += extendedprice;
                        float fact = extendedprice * (1 - discount);
                        values[2] += fact;
                        values[3] += fact * (1 + tax);
                        values[4] += quantity;
                        values[5] += extendedprice;
                        values[6] += discount;
                        values[7]++;
                        selectivity++;
                    }
                }
            }
        }
        fileReader.close();
        //        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        System.out.println("selectivity: " + selectivity);
        System.out.println("Result:");
        for (Map.Entry<String, float[]> entry : gbyMap.entrySet()) {
            System.out.println(entry.getKey().charAt(0) + "," + entry.getKey().charAt(1) + ":" + entry.getValue()[0]
                    + "," + entry.getValue()[1] + "," + entry.getValue()[2] + "," + entry.getValue()[3] + ","
                    + entry.getValue()[4] / entry.getValue()[7] + "," + entry.getValue()[5] / entry.getValue()[7] + ","
                    + entry.getValue()[6] / entry.getValue()[7] + "," + (long) entry.getValue()[7]);
        }
    }

    public static void trevniExecutor(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String t1 = args[2]; //l_shipdate
        long start = System.currentTimeMillis();
        Params param = new Params(file);
        param.setSchema(readSchema);
        AvroColumnReader<Record> reader = new AvroColumnReader<Record>(param);
        int count = 0;
        double result = 0.00;
        byte[] tmp = new byte[2];
        long selectivity = 0;
        Map<String, float[]> gbyMap = new HashMap<>();
        while (reader.hasNext()) {
            Record c = reader.next();
            List<Record> os = (List<Record>) c.get(0);
            for (Record o : os) {
                List<Record> ls = (List<Record>) o.get(0);
                for (Record r : ls) {
                    if (((String) r.get(6)).compareTo(t1) <= 0) {
                        float quantity = (float) r.get(0);
                        float extendedprice = (float) r.get(1);
                        float discount = (float) r.get(2);
                        float tax = (float) r.get(3);
                        byte[] returnflag = ((ByteBuffer) r.get(4)).array();
                        byte[] linestatus = ((ByteBuffer) r.get(5)).array();
                        if (returnflag.length != 1 || linestatus.length != 1) {
                            System.out.println("need to handle this." + " rf: " + returnflag[0] + " out of: "
                                    + returnflag.length + " ls: " + linestatus[0] + " out of: " + linestatus.length
                                    + " tk: " + selectivity);
                        } else {
                            //System.out.println(selectivity + " out of: " + reader.getRowCount(0));
                        }
                        tmp[0] = returnflag[0];
                        tmp[1] = linestatus[0];
                        String bb = new String(tmp);
                        if (!gbyMap.containsKey(bb)) {
                            byte[] key = new byte[2];
                            key[0] = tmp[0];
                            key[1] = tmp[1];
                            gbyMap.put(new String(key), new float[8]);
                        }
                        float[] values = gbyMap.get(bb);
                        values[0] += quantity;
                        values[1] += extendedprice;
                        float fact = extendedprice * (1 - discount);
                        values[2] += fact;
                        values[3] += fact * (1 + tax);
                        values[4] += quantity;
                        values[5] += extendedprice;
                        values[6] += discount;
                        values[7]++;
                        selectivity++;
                    }
                }
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("selectivity: " + selectivity);
        System.out.println("Result:");
        for (Map.Entry<String, float[]> entry : gbyMap.entrySet()) {
            System.out.println(entry.getKey().charAt(0) + "," + entry.getKey().charAt(1) + ":" + entry.getValue()[0]
                    + "," + entry.getValue()[1] + "," + entry.getValue()[2] + "," + entry.getValue()[3] + ","
                    + entry.getValue()[4] / entry.getValue()[7] + "," + entry.getValue()[5] / entry.getValue()[7] + ","
                    + entry.getValue()[6] / entry.getValue()[7] + "," + (long) entry.getValue()[7]);
        }
    }

    public static void parquetExecutor(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String t1 = args[2]; //l_shipdate
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<GenericRecord>();
        readSupport.setRequestedProjection(conf, readSchema);
        readSupport.setAvroReadSchema(conf, readSchema);
        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> reader = new ParquetReader(conf, new Path(args[0]), readSupport);
        int count = 0;
        double result = 0.00;
        byte[] tmp = new byte[2];
        long selectivity = 0;
        Map<String, float[]> gbyMap = new HashMap<>();
        GenericRecord c = reader.read();
        while (c != null) {
            List<Record> os = (List<Record>) c.get(0);
            for (Record o : os) {
                List<Record> ls = (List<Record>) o.get(0);
                for (Record r : ls) {
                    if (r.get(6).toString().compareTo(t1) <= 0) {
                        float quantity = (float) r.get(0);
                        float extendedprice = (float) r.get(1);
                        float discount = (float) r.get(2);
                        float tax = (float) r.get(3);
                        byte[] returnflag = ((ByteBuffer) r.get(4)).array();
                        byte[] linestatus = ((ByteBuffer) r.get(5)).array();
                        if (returnflag.length != 1 || linestatus.length != 1) {
                            System.out.println("need to handle this." + " rf: " + returnflag[0] + " out of: "
                                    + returnflag.length + " ls: " + linestatus[0] + " out of: " + linestatus.length
                                    + " tk: " + selectivity);
                        } else {
                            //System.out.println(selectivity + " out of: " + reader.getRowCount(0));
                        }
                        tmp[0] = returnflag[0];
                        tmp[1] = linestatus[0];
                        String bb = new String(tmp);
                        if (!gbyMap.containsKey(bb)) {
                            byte[] key = new byte[2];
                            key[0] = tmp[0];
                            key[1] = tmp[1];
                            gbyMap.put(new String(key), new float[8]);
                        }
                        float[] values = gbyMap.get(bb);
                        values[0] += quantity;
                        values[1] += extendedprice;
                        float fact = extendedprice * (1 - discount);
                        values[2] += fact;
                        values[3] += fact * (1 + tax);
                        values[4] += quantity;
                        values[5] += extendedprice;
                        values[6] += discount;
                        values[7]++;
                        selectivity++;
                    }
                }
            }
            c = reader.read();
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("selectivity: " + selectivity);
        System.out.println("Result:");
        for (Map.Entry<String, float[]> entry : gbyMap.entrySet()) {
            System.out.println(entry.getKey().charAt(0) + "," + entry.getKey().charAt(1) + ":" + entry.getValue()[0]
                    + "," + entry.getValue()[1] + "," + entry.getValue()[2] + "," + entry.getValue()[3] + ","
                    + entry.getValue()[4] / entry.getValue()[7] + "," + entry.getValue()[5] / entry.getValue()[7] + ","
                    + entry.getValue()[6] / entry.getValue()[7] + "," + (long) entry.getValue()[7]);
        }
    }

    public static void coresExecutor(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file);
        long t2 = System.currentTimeMillis();
        reader.createFilterRead(max);
        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        //byte[] key = new byte[2];
        byte[] tmp = new byte[2];
        long selectivity = 0;
        Map<String, float[]> gbyMap = new HashMap<>();
        while (reader.hasNext()) {
            Record r = reader.next();
            if (((String) r.get(6)).compareTo(args[3]) <= 0) {
                float quantity = (float) r.get(0);
                float extendedprice = (float) r.get(1);
                float discount = (float) r.get(2);
                float tax = (float) r.get(3);
                byte[] returnflag = ((ByteBuffer) r.get(4)).array();
                byte[] linestatus = ((ByteBuffer) r.get(5)).array();
                if (returnflag.length != 1 || linestatus.length != 1) {
                    System.out.println(
                            "need to handle this." + " rf: " + returnflag[0] + " out of: " + returnflag.length + " ls: "
                                    + linestatus[0] + " out of: " + linestatus.length + " tk: " + selectivity);
                }
                tmp[0] = returnflag[0];
                tmp[1] = linestatus[0];
                String bb = new String(tmp);
                if (!gbyMap.containsKey(bb)) {
                    byte[] key = new byte[2];
                    key[0] = tmp[0];
                    key[1] = tmp[1];
                    gbyMap.put(new String(key), new float[8]);
                }
                float[] values = gbyMap.get(bb);
                values[0] += quantity;
                values[1] += extendedprice;
                float fact = extendedprice * (1 - discount);
                values[2] += fact;
                values[3] += fact * (1 + tax);
                values[4] += quantity;
                values[5] += extendedprice;
                values[6] += discount;
                values[7]++;
                selectivity++;
            }
        }
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("selectivity: " + selectivity);
        System.out.println("Result:");
        for (Map.Entry<String, float[]> entry : gbyMap.entrySet()) {
            System.out.println(entry.getKey().charAt(0) + "," + entry.getKey().charAt(1) + ":" + entry.getValue()[0]
                    + "," + entry.getValue()[1] + "," + entry.getValue()[2] + "," + entry.getValue()[3] + ","
                    + entry.getValue()[4] / entry.getValue()[7] + "," + entry.getValue()[5] / entry.getValue()[7] + ","
                    + entry.getValue()[6] / entry.getValue()[7] + "," + (long) entry.getValue()[7]);
        }
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        switch (args[0].substring(args[0].length() - 4, args[0].length())) {
            case "trev":
                trevniExecutor(args);
                break;
            case "parq":
                trevniExecutor(args);
                break;
            case "avro":
                avroExecutor(args);
                break;
            case "neci":
                coresExecutor(args);
                break;
            default:
                throw new TrevniRuntimeException("args: " + args[0]);
        }
    }

}
