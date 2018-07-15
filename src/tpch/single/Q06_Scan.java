/**
 * 
 */
package tpch.single;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
public class Q06_Scan {
    public static void avroExecutor(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String t1 = args[2]; //l_shipdate
        String t2 = args[3];
        float d1 = Float.parseFloat(args[4]); //l_discount
        float d2 = Float.parseFloat(args[5]);
        float q = Float.parseFloat(args[6]); //l_quantity
        long start = System.currentTimeMillis();
        DatumReader<Record> reader = new GenericDatumReader<Record>(readSchema);
        DataFileReader<Record> fileReader = new DataFileReader<Record>(file, reader);
        int count = 0;
        double result = 0.00;
        while (fileReader.hasNext()) {
            Record r = fileReader.next();
            List<Record> psl = (List<Record>) r.get(0);
            List<Record> l = new ArrayList<Record>();
            for (Record m : psl) {
                l.addAll((List<Record>) m.get(0));
            }
            for (Record m : l) {
                String date = m.get("l_shipdate").toString();
                float dis = (float) m.get("l_discount");
                float quan = (float) m.get("l_quantity");
                float price = (float) m.get("l_extendedprice");
                //            result += (float) r.get(0) * (float) r.get(1);
                if (date.compareTo(t1) >= 0 && date.compareTo(t2) < 0 && dis >= d1 && dis <= d2 && quan < q) {
                    result += price * dis;
                    count++;
                }
            }
        }
        fileReader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result) + " count: " + count);
    }

    public static void trevniExecutor(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String t1 = args[2]; //l_shipdate
        String t2 = args[3];
        float d1 = Float.parseFloat(args[4]); //l_discount
        float d2 = Float.parseFloat(args[5]);
        float q = Float.parseFloat(args[6]); //l_quantity
        long start = System.currentTimeMillis();
        Params param = new Params(file);
        param.setSchema(readSchema);
        AvroColumnReader<Record> reader = new AvroColumnReader<Record>(param);
        int count = 0;
        double result = 0.00;
        while (reader.hasNext()) {
            Record r = reader.next();
            List<Record> psl = (List<Record>) r.get(0);
            List<Record> l = new ArrayList<Record>();
            for (Record m : psl) {
                l.addAll((List<Record>) m.get(0));
            }
            for (Record m : l) {
                String date = m.get("l_shipdate").toString();
                float dis = (float) m.get("l_discount");
                float quan = (float) m.get("l_quantity");
                float price = (float) m.get("l_extendedprice");
                //            result += (float) r.get(0) * (float) r.get(1);
                if (date.compareTo(t1) >= 0 && date.compareTo(t2) < 0 && dis >= d1 && dis <= d2 && quan < q) {
                    result += price * dis;
                    count++;
                }
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result) + " count: " + count);
    }

    public static void parquetExecutor(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String t1 = args[2]; //l_shipdate
        String t2 = args[3];
        float d1 = Float.parseFloat(args[4]); //l_discount
        float d2 = Float.parseFloat(args[5]);
        float q = Float.parseFloat(args[6]); //l_quantity
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<GenericRecord>();
        readSupport.setRequestedProjection(conf, readSchema);
        readSupport.setAvroReadSchema(conf, readSchema);
        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> reader = new ParquetReader(conf, new Path(args[0]), readSupport);
        int count = 0;
        double result = 0.00;
        GenericRecord r = reader.read();
        while (r != null) {
            List<Record> psl = (List<Record>) r.get(0);
            List<Record> l = new ArrayList<Record>();
            for (Record m : psl) {
                l.addAll((List<Record>) m.get(0));
            }
            for (Record m : l) {
                String date = m.get("l_shipdate").toString();
                float dis = (float) m.get("l_discount");
                float quan = (float) m.get("l_quantity");
                float price = (float) m.get("l_extendedprice");
                //            result += (float) r.get(0) * (float) r.get(1);
                if (date.compareTo(t1) >= 0 && date.compareTo(t2) < 0 && dis >= d1 && dis <= d2 && quan < q) {
                    result += price * dis;
                    count++;
                }
            }
            r = reader.read();
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result) + " count: " + count);
    }

    public static void coresExecutor(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        String t1 = args[3]; //l_shipdate
        String t2 = args[4];
        float d1 = Float.parseFloat(args[5]); //l_discount
        float d2 = Float.parseFloat(args[6]);
        float q = Float.parseFloat(args[7]); //l_quantity
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file);
        reader.createSchema(readSchema);
        reader.createRead(max);
        int count = 0;
        double result = 0.00;
        while (reader.hasNext()) {
            Record r = reader.next();
            List<Record> psl = (List<Record>) r.get(0);
            List<Record> l = new ArrayList<Record>();
            for (Record m : psl) {
                l.addAll((List<Record>) m.get(0));
            }
            for (Record m : l) {
                String date = m.get("l_shipdate").toString();
                float dis = (float) m.get("l_discount");
                float quan = (float) m.get("l_quantity");
                float price = (float) m.get("l_extendedprice");
                //            result += (float) r.get(0) * (float) r.get(1);
                if (date.compareTo(t1) >= 0 && date.compareTo(t2) < 0 && dis >= d1 && dis <= d2 && quan < q) {
                    result += price * dis;
                    count++;
                }
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result) + " count: " + count);
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
                parquetExecutor(args);
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
