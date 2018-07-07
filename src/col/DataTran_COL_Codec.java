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
public class DataTran_COL_Codec extends DataTran_COL
{

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String path = args[0];
        String result = args[1] + "result";
        String schema = args[1] + "lay";
        int free = Integer.parseInt(args[2]);
        int mul = Integer.parseInt(args[3]);
        int max = Integer.parseInt(args[4]);

        int[] fields0 = new int[] { 0, 3 };
        long start = System.currentTimeMillis();
        lSort(path + "lineitem.tbl", schema + "1/single.avsc", fields0, result + "1/", free, mul);
        long end = System.currentTimeMillis();
        System.out.println("+++++++lineitem sort time+++++++" + (end - start));

        int[] fields1 = new int[] { 0 };
        int[] fields2 = new int[] { 0 };
        int[] fields3 = new int[] { 1, 0 };

        start = System.currentTimeMillis();
        lSort(path + "orders.tbl", schema + "2/single.avsc", fields1, result + "2/", free, mul);
        end = System.currentTimeMillis();
        System.out.println("+++++++orders sort time+++++++" + (end - start));

        start = System.currentTimeMillis();
        doublePri(result + "2/", result + "1/", schema + "2/", schema + "1/single.avsc", fields1, fields2, fields3,
                result + "3/", free, mul);
        end = System.currentTimeMillis();
        System.out.println("+++++++orders&&lineitem time+++++++" + (end - start));

        int[] fields4 = new int[] { 0 };
        int[] fields5 = new int[] { 1 };
        start = System.currentTimeMillis();
        int index = finalTran(path + "customer.tbl", result + "3/", schema + "3/", schema + "2/nest.avsc", fields4,
                fields5, result + "/", max, mul);
        end = System.currentTimeMillis();
        System.out.println("+++++++customer&&orders&&lineitem time+++++++" + (end - start) + " index: " + index);

        String resultPath = result + "/";
        Schema s = new Schema.Parser().parse(new File(schema + "3/" + "nest.avsc"));
        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, resultPath, max, mul, args[5]);
        File[] files = new File[index];
        for (int i = 0; i < index; i++)
            files[i] = new File(resultPath + "file" + String.valueOf(i) + ".neci");
        //        if (230 == 1) {
        //            new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
        //            new File(resultPath + "file0.neci").renameTo(new File(resultPath + "result.neci"));
        //        } else {
        writer.mergeFiles(files);
        //        }
        System.out.println("merge completed!");
    }

}
