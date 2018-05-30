package q9;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

public class JoinOperator {
    private File[] files1;
    private File[] files2;
    private Schema s1;
    private Schema s2;
    private int keyNO;
    private boolean dsc;

    private Schema schema;
    private List<Field> fs;
    private String path;
    private SortOperator SO;
    private int[] resIn;
    static int max = 100000;

    public JoinOperator(File[] files1, File[] files2, Schema s1, Schema s2, int keyNO, boolean dsc, Schema schema,
            String path) {
        this.files1 = files1;
        this.files2 = files2;
        this.s1 = s1;
        this.s2 = s2;
        this.keyNO = keyNO;
        this.dsc = dsc;

        this.schema = schema;
        fs = schema.getFields();
        this.path = path;

        comRes();
    }

    public JoinOperator(File[] files1, File[] files2, Schema s1, Schema s2, int keyNO, boolean dsc, Schema schema,
            String path, SortOperator so) {
        this(files1, files2, s1, s2, keyNO, dsc, schema, path);
        SO = so;
    }

    private void comRes() {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        List<Field> fs = s1.getFields();
        for (int i = 0; i < fs.size(); i++) {
            map.put(fs.get(i).name(), 0);
        }
        fs = s2.getFields();
        for (int i = keyNO; i < fs.size(); i++) {
            map.put(fs.get(i).name(), 1);
        }

        fs = schema.getFields();
        resIn = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            resIn[i] = map.get(fs.get(i).name());
        }
    }

    public void setMax(int max) {
        this.max = max;
    }

    public void halfJoin() throws IOException {
        if (SO != null) {
            halfJoinWithSort();
            return;
        }
        SortKeyReader reader1 = new SortKeyReader(s1, keyNO, files1, dsc);
        SortKeyReader reader2 = new SortKeyReader(s2, keyNO, files2, dsc);
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path + "/file")));

        SortKey key2 = reader2.next();
        if (dsc) {
            while (reader1.hasNext()) {
                SortKey key1 = reader1.next();
                while (reader2.hasNext() && key2.compareTo(key1) < 0) {
                    key2 = reader2.next();
                }
                Record r;
                if (key2 != null && key1.compareTo(key2) == 0) {
                    r = getRes(key1, key2);
                    if (reader2.hasNext())
                        key2 = reader2.next();
                    else
                        key2 = null;
                } else {
                    r = getRes(key1);
                }
                writer.write(getString(r));
                writer.write(System.getProperties().getProperty("line.separator"));
            }
        } else {
            while (reader1.hasNext()) {
                SortKey key1 = reader1.next();
                while (reader2.hasNext() && key2.compareTo(key1) > 0) {
                    key2 = reader2.next();
                }
                Record r;
                if (key2 != null && key1.compareTo(key2) == 0) {
                    r = getRes(key1, key2);
                    if (reader2.hasNext())
                        key2 = reader2.next();
                    else
                        key2 = null;
                } else {
                    r = getRes(key1);
                }
                writer.write(getString(r));
                writer.write(System.getProperties().getProperty("line.separator"));
            }
        }
        writer.close();
    }

    private void halfJoinWithSort() throws IOException {
        SortKeyReader reader1 = new SortKeyReader(s1, keyNO, files1, dsc);
        SortKeyReader reader2 = new SortKeyReader(s2, keyNO, files2, dsc);

        SortKey key2 = reader2.next();
        if (dsc) {
            while (reader1.hasNext()) {
                SortKey key1 = reader1.next();
                while (reader2.hasNext() && key2.compareTo(key1) < 0) {
                    key2 = reader2.next();
                }
                Record r;
                if (key2 != null && key1.compareTo(key2) == 0) {
                    r = getRes(key1, key2);
                    if (reader2.hasNext())
                        key2 = reader2.next();
                    else
                        key2 = null;
                } else {
                    r = getRes(key1);
                }
                SO.add(r);
            }
        } else {
            while (reader1.hasNext()) {
                SortKey key1 = reader1.next();
                while (key2.compareTo(key1) > 0) {
                    key2 = reader2.next();
                }
                Record r;
                if (key1.compareTo(key2) == 0) {
                    r = getRes(key1, key2);
                    key2 = reader2.next();
                } else {
                    r = getRes(key1);
                }
                SO.add(r);
            }
        }
        SO.flush();
    }

    public Record getRes(SortKey key1, SortKey key2) {
        Record r = new Record(schema);
        for (int i = 0; i < fs.size(); i++) {
            Field f = fs.get(i);
            if (resIn[i] == 0) {
                r.put(i, key1.getRecord().get(f.name()));
            } else {
                r.put(i, key2.getRecord().get(f.name()));
            }
        }
        return r;
    }

    public Record getRes(SortKey key1) {
        Record r = new Record(schema);
        for (int i = 0; i < fs.size(); i++) {
            Field f = fs.get(i);
            if (resIn[i] == 0) {
                r.put(i, key1.getRecord().get(f.name()));
            }
        }
        return r;
    }

    public static String getString(Record r) {
        StringBuilder res = new StringBuilder();
        res.append(r.get(0));
        for (int i = 1; i < r.getSchema().getFields().size(); i++) {
            res.append("|" + r.get(i));
        }
        return res.toString();
    }
}
