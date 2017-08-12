package q17;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.FilterBatchColumnReader;
import cores.avro.FilterOperator;

public class Q17 {
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        FilterOperator[] filters = new FilterOperator[2];
        filters[0] = new Pfilter1(args[3], args[4]);
        int contain = Integer.parseInt(args[5]);
        String[] com = new String[contain];
        int i = 6;
        for (int m = 0; m < contain; m++) {
            com[m] = args[i + m];
        }
        filters[1] = new Pfilter2(com);
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file, filters);
        reader.createSchema(readSchema);
        long t1 = System.currentTimeMillis();
        reader.filter();
        long t2 = System.currentTimeMillis();
        reader.createFilterRead(max);
        int count = 0;
        int sumC = reader.getRowCount(0);
        double result = 0.00;
        while (reader.hasNext()) {
            Record r = reader.next();
            double quantity = 0.00;
            List<Record> psL = (List<Record>) r.get(0);
            List<Record> lL = new ArrayList<Record>();
            for (Record ps : psL) {
                lL.addAll((List<Record>) ps.get(0));
            }
            for (Record l : lL) {
                quantity += (float) l.get(0);
            }
            quantity = quantity / lL.size() * 0.2;

            for (Record l : lL) {
                float q = (float) l.get(0);
                if (q < quantity)
                    result += (float) l.get(1);
            }
            count++;
        }
        reader.close();
        result /= 7.0;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println(sumC);
        System.out.println("time: " + (end - start));
        System.out.println("filter time: " + (t2 - t1));
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("avg_yearly: " + nf.format(result));
    }
}
