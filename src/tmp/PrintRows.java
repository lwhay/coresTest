package tmp;

import java.io.File;
import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;

import cores.avro.BatchColumnReader;

public class PrintRows {
    public static void main(String[] args) throws IOException {
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(new File(args[0]));
        for (int i = 0; i < 3; i++) {
            System.out.println("the " + i + " level rowCount:" + reader.getLevelRowCount(i));
        }
        reader.close();
    }
}
