package q9;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.FilterBatchColumnReader;

public class test {
    public static void sk_year1(File colFile, Schema colSchema, int max, String syPath, Schema sySchema)
            throws IOException {
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(colFile);
        reader.createSchema(colSchema);
        reader.createRead(max);
        while (reader.hasNext()) {
            Record r = reader.next();
        }
        reader.close();
    }

    public static void sk_year2(File colFile, Schema colSchema, int max, String syPath, Schema sySchema)
            throws IOException {
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(colFile);
        reader.createSchema(colSchema);
        reader.createRead(max);
        while (reader.hasNext()) {
            Record r = reader.next();
            String o_orderdate = r.get("o_orderdate").toString();
            int year = Integer.parseInt(o_orderdate.substring(0, 4));
            List<Record> lineitemList = (List<Record>) r.get(1);
            for (Record l : lineitemList) {
                int l_suppkey = (int) l.get("l_suppkey");
                Record sy = new Record(sySchema);
                sy.put(0, l_suppkey);
                sy.put(1, year);
                SortKey sk = new SortKey(sy, 1);
            }
        }
        reader.close();
    }

    public static void sk_year3(File colFile, Schema colSchema, int max, String syPath, Schema sySchema)
            throws IOException {
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(colFile);
        reader.createSchema(colSchema);
        reader.createRead(max);
        SortOperator SO = new SortOperator(syPath, true, 1);
        HashSet<SortKey> batch = new HashSet<SortKey>();
        while (reader.hasNext()) {
            Record r = reader.next();
            String o_orderdate = r.get("o_orderdate").toString();
            int year = Integer.parseInt(o_orderdate.substring(0, 4));
            List<Record> lineitemList = (List<Record>) r.get(1);
            for (Record l : lineitemList) {
                int l_suppkey = (int) l.get("l_suppkey");
                Record sy = new Record(sySchema);
                sy.put(0, l_suppkey);
                sy.put(1, year);
                SortKey sk = new SortKey(sy, 1);
                if (!batch.contains(sk))
                    batch.add(sk);
            }
            if (batch.size() >= max) {
                SO.flush(new ArrayList<SortKey>(batch));
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            SO.flush(new ArrayList<SortKey>(batch));
            batch.clear();
        }
        reader.close();
    }

    public static void main(String[] args) throws IOException {
        System.out.println("[path] [schemaPath] [max] [part]");
        String path = args[0];
        String schemaPath = args[1];

        int max = Integer.parseInt(args[2]);
        int part = Integer.parseInt(args[3]);

        long start = System.nanoTime();
        File colFile = new File(path + "/tpch/col/result.neci");
        String syPath = path + "/syPath";
        Schema colSchema = new Schema.Parser().parse(new File(schemaPath + "/q9_col.avsc"));
        Schema sySchema = new Schema.Parser().parse(new File(schemaPath + "/sk_year.avsc"));
        if (part == 1)
            sk_year1(colFile, colSchema, max, syPath, sySchema);
        else if (part == 2)
            sk_year2(colFile, colSchema, max, syPath, sySchema);
        else
            sk_year3(colFile, colSchema, max, syPath, sySchema);
        long end = System.nanoTime();
        System.out.println("#######sk_year part " + part + " time: " + (end - start) + " ns");
    }

}
