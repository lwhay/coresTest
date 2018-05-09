package q9;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

public class SortOperator {
    private String path;
    private int index;
    private int keyNO;
    private boolean dsc;

    private ArrayList<SortKey> batch;
    private static int max = 100000;

    public SortOperator(String path, boolean dsc) {
        this.path = path;
        index = 0;
        batch = new ArrayList<SortKey>();
        this.dsc = dsc;
    }

    public SortOperator(String path, boolean dsc, int keyNO) {
        this(path, dsc);
        this.keyNO = keyNO;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public void add(Record r) throws IOException {
        batch.add(new SortKey(r, keyNO));
        if (batch.size() >= max) {
            flush();
        }
    }

    public int flush() throws IOException {
        assert (!batch.isEmpty());
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path + "/part" + index)));
        Collections.sort(batch);
        if (dsc) {
            for (int i = 0; i < batch.size(); i++) {
                writer.write(batch.get(i).getValue());
                writer.write(System.getProperties().getProperty("line.separator"));
            }
        } else {
            for (int i = batch.size() - 1; i >= 0; i--) {
                writer.write(batch.get(i).getValue());
                writer.write(System.getProperties().getProperty("line.separator"));
            }
        }
        writer.close();
        batch.clear();
        index++;
        return index;
    }

    public int flush(ArrayList<SortKey> sks) throws IOException {
        assert (!sks.isEmpty());
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path + "/part" + index)));
        Collections.sort(sks);
        if (dsc) {
            for (int i = 0; i < sks.size(); i++) {
                writer.write(sks.get(i).getValue());
                writer.write(System.getProperties().getProperty("line.separator"));
            }
        } else {
            for (int i = sks.size() - 1; i >= 0; i--) {
                writer.write(sks.get(i).getValue());
                writer.write(System.getProperties().getProperty("line.separator"));
            }
        }
        writer.close();
        index++;
        return index;
    }

    public void sumMerge(Schema s) throws IOException {
        assert (index > 0);
        File[] files = new File[index];
        for (int i = 0; i < index; i++) {
            files[i] = new File(path + "/part" + i);
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path + "/file")));
        SortKeyReader reader = new SortKeyReader(s, keyNO, files, dsc);
        SortKey key = reader.next();
        while (reader.hasNext()) {
            SortKey key2 = reader.next();
            if (key.equals(key2)) {
                getSum(key, key2, keyNO);
            } else {
                writer.write(key.getValue());
                writer.write(System.getProperties().getProperty("line.separator"));
                key = key2;
            }
        }
        writer.write(key.getValue());
        writer.write(System.getProperties().getProperty("line.separator"));
        writer.close();
    }

    public ArrayList<SortKey> sumMergeToMem(Schema s) throws IOException {
        assert (index > 0);
        ArrayList<SortKey> res = new ArrayList<SortKey>();
        File[] files = new File[index];
        for (int i = 0; i < index; i++) {
            files[i] = new File(path + "/part" + i);
        }
        SortKeyReader reader = new SortKeyReader(s, keyNO, files, dsc);
        SortKey key = reader.next();
        while (reader.hasNext()) {
            SortKey key2 = reader.next();
            if (key.equals(key2)) {
                getSum(key, key2, keyNO);
            } else {
                res.add(key);
                key = key2;
            }
        }
        res.add(key);
        return res;
    }

    public static ArrayList<SortKey> sumMerge(ArrayList<SortKey> batch, int keyNO) {
        assert (!batch.isEmpty());
        ArrayList<SortKey> res = new ArrayList<SortKey>();
        Iterator<SortKey> itsk = batch.iterator();
        SortKey key = itsk.next();
        while (itsk.hasNext()) {
            SortKey key2 = itsk.next();
            if (key.equals(key2)) {
                getSum(key, key2, keyNO);
            } else {
                res.add(key);
                key = key2;
            }
        }
        res.add(key);
        return res;
    }

    private static SortKey getSum(SortKey key1, SortKey key2, int keyNO) {
        Record r1 = key1.getRecord();
        Record r2 = key2.getRecord();
        Field f = r1.getSchema().getFields().get(keyNO);
        switch (f.schema().getType()) {
            case INT:
                int s1 = Integer.parseInt(r1.get(keyNO).toString()) + Integer.parseInt(r2.get(keyNO).toString());
                r1.put(keyNO, s1);
                break;
            case LONG:
                long s2 = Long.parseLong(r1.get(keyNO).toString()) + Long.parseLong(r2.get(keyNO).toString());
                r1.put(keyNO, s2);
                break;
            case FLOAT:
                float s3 = Float.parseFloat(r1.get(keyNO).toString()) + Float.parseFloat(r2.get(keyNO).toString());
                r1.put(keyNO, s3);
                break;
            case DOUBLE:
                double s4 = Double.parseDouble(r1.get(keyNO).toString()) + Double.parseDouble(r2.get(keyNO).toString());
                r1.put(keyNO, s4);
                break;
            default:
                throw new ClassCastException("This type is not supported for Key type: " + f.schema());
        }
        return key1;
    }
}
