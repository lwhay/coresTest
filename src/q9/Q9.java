package q9;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import PhysicalOperators.SortMergeJoinOperator;
import PhysicalOperators.Pfilter;
import PhysicalOperators.SortOperator;
import cores.avro.FilterBatchColumnReader;
import cores.avro.FilterOperator;

public class Q9 {
    //读取(ps+l)数据，计算(sk+amount)，以sk升序排列
    public static File[] sk_amount(File ppslFile, Schema ppslSchema, int max, String color, String saPath,
            Schema saSchema) throws IOException {
        FilterOperator[] filters = new FilterOperator[1];
        Pfilter f = new Pfilter(color);
        filters[0] = f;
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(ppslFile, filters);
        reader.createSchema(ppslSchema);
        reader.filter();
        reader.createFilterRead(max);

        SortOperator SO = new SortOperator(saPath, true, 1);
        ArrayList<SortKey> batch = new ArrayList<SortKey>();
        while (reader.hasNext()) {
            Record r = reader.next();
            int ps_suppkey = (int) r.get("ps_suppkey");
            float ps_supplycost = (float) r.get("ps_supplycost");
            List<Record> lineitemList = (List<Record>) r.get(2);
            Record sa = new Record(saSchema);
            double amount = 0.00;
            sa.put(0, ps_suppkey);
            for (Record l : lineitemList) {
                float l_quantity = (float) l.get("l_quantity");
                float l_extendedprice = (float) l.get("l_extendedprice");
                float l_discount = (float) l.get("l_discount");
                double a = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity;
                amount += a;
            }
            sa.put(1, amount);
            batch.add(new SortKey(sa, 1));
            if (batch.size() >= max) {
                SO.flush(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            SO.flush(batch);
            batch.clear();
        }
        reader.close();

        long start = System.currentTimeMillis();
        SO.sumMerge(saSchema);
        long end = System.currentTimeMillis();
        System.out.println("sk_amount merge time:" + (end - start));
        return new File[] { new File(saPath + "/file") };
    }

    //读取(o+l)数据，取(sk+year)，以sk升序排列
    public static File[] sk_year(File colFile, Schema colSchema, int max, String syPath, Schema sySchema)
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
        int index = SO.getIndex();
        File[] res = new File[index];
        for (int i = 0; i < index; i++) {
            res[i] = new File(syPath + "/part" + i);
        }
        return res;
    }

    public static void sk_year_amount(File[] sa, Schema saSchema, File[] sy, Schema sySchema, String syaPath,
            Schema syaSchema) throws IOException {
        SortMergeJoinOperator JO = new SortMergeJoinOperator(sa, sy, saSchema, sySchema, 1, true, syaSchema, syaPath);
        JO.halfJoin();
    }

    public static void sk_nk(File sFile, Schema sSchema, int max, String snPath) throws IOException {
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(sFile);
        reader.createSchema(sSchema);
        reader.createRead(max);
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(snPath + "/file")));
        while (reader.hasNext()) {
            Record r = reader.next();
            writer.write(SortMergeJoinOperator.getString(r));
            writer.write(System.getProperties().getProperty("line.separator"));
        }
        reader.close();
        writer.close();
    }

    public static ArrayList<SortKey> nk_year_amount(File sya, Schema syaSchema, File sn, Schema snSchema,
            String nyaPath, Schema nyaSchema) throws IOException {
        File[] syaF = { sya };
        File[] snF = { sn };
        SortOperator SO = new SortOperator(nyaPath, true, 2);
        SortMergeJoinOperator JO = new SortMergeJoinOperator(syaF, snF, syaSchema, snSchema, 1, true, nyaSchema, nyaPath, SO);
        JO.halfJoin();
        long start = System.currentTimeMillis();
        ArrayList<SortKey> nya = SO.sumMergeToMem(nyaSchema);
        long end = System.currentTimeMillis();
        System.out.println("nk_year_amount merge time:" + (end - start));
        return nya;
    }

    public static void nation_year_amount(ArrayList<SortKey> nya, Schema nayaSchema, String nPath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(nPath)));
        HashMap<Integer, String> map = new HashMap<Integer, String>();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            int nk = Integer.parseInt(tmp[0]);
            map.put(nk, tmp[1]);
        }
        reader.close();
        ArrayList<SortKey> res = new ArrayList<SortKey>();
        for (SortKey sk : nya) {
            Record skr = sk.getRecord();
            Record r = new Record(nayaSchema);
            r.put(0, map.get((int) skr.get(0)));
            r.put(1, skr.get(1));
            r.put(2, skr.get(2));
            res.add(new SortKey(r, 2));
        }
        Collections.sort(res);
        //res = SortOperator.sumMerge(res, 2);
        for (int i = res.size() - 1; i >= 0; i--) {
            SortKey sk = res.get(i);
            System.out.println(sk.getValue());
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("[path] [schemaPath] [max] [color]");
        String path = args[0];
        String schemaPath = args[1];

        File ppslFile = new File(path + "/tpch/ppsl/result.neci");
        int max = Integer.parseInt(args[2]);
        String color = args[3];
        String saPath = path + "/saPath";
        Schema ppslSchema = new Schema.Parser().parse(new File(schemaPath + "/q9_ppsl.avsc"));
        Schema saSchema = new Schema.Parser().parse(new File(schemaPath + "/sk_amount.avsc"));
        long start = System.currentTimeMillis();
        File[] sa = sk_amount(ppslFile, ppslSchema, max, color, saPath, saSchema);
        long end = System.currentTimeMillis();
        System.out.println("#######sk_amount time: " + (end - start));

        File colFile = new File(path + "/tpch/col/result.neci");
        String syPath = path + "/syPath";
        Schema colSchema = new Schema.Parser().parse(new File(schemaPath + "/q9_col.avsc"));
        Schema sySchema = new Schema.Parser().parse(new File(schemaPath + "/sk_year.avsc"));
        File[] sy = sk_year(colFile, colSchema, max, syPath, sySchema);
        start = System.currentTimeMillis();
        System.out.println("#######sk_year time: " + (start - end));

        String syaPath = path + "/syaPath";
        Schema syaSchema = new Schema.Parser().parse(new File(schemaPath + "/sk_year_amount.avsc"));
        sk_year_amount(sa, saSchema, sy, sySchema, syaPath, syaSchema);
        end = System.currentTimeMillis();
        System.out.println("#######sk_year_amount time: " + (end - start));

        File sFile = new File(path + "/tpch/s/result/result.neci");
        String snPath = path + "/snPath";
        Schema sSchema = new Schema.Parser().parse(new File(schemaPath + "/q9_s.avsc"));
        Schema snSchema = new Schema.Parser().parse(new File(schemaPath + "/sk_nk.avsc"));
        sk_nk(sFile, sSchema, max, snPath);
        start = System.currentTimeMillis();
        System.out.println("#######sk_nk time: " + (start - end));

        String nyaPath = path + "/nyaPath";
        Schema nyaSchema = new Schema.Parser().parse(new File(schemaPath + "/nk_year_amount.avsc"));
        ArrayList<SortKey> nya = nk_year_amount(new File(syaPath + "/file"), syaSchema, new File(snPath + "/file"),
                snSchema, nyaPath, nyaSchema);
        end = System.currentTimeMillis();
        System.out.println("#######nk_year_amount time: " + (end - start));

        String nPath = path + "/tpch/nation.tbl";
        Schema nayaSchema = new Schema.Parser().parse(new File(schemaPath + "/nation_year_amount.avsc"));
        nation_year_amount(nya, nayaSchema, nPath);
        start = System.currentTimeMillis();
        System.out.println("#######nation_year_amount time: " + (start - end));
    }
}
