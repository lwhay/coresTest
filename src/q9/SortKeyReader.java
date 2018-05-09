package q9;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

public class SortKeyReader {
    private Schema s;
    private List<Field> fs;
    private int keyNO;
    private File[] files;
    private boolean dsc;
    private BufferedReader[] readers;
    private SortKey[] sks;
    private int[] index;
    private int start;

    public SortKeyReader(Schema s, int keyNO, File[] files, boolean dsc) throws IOException {
        this.s = s;
        fs = s.getFields();
        this.keyNO = keyNO;
        this.files = files;
        this.dsc = dsc;
        start = 0;

        create();
    }

    private void create() throws IOException {
        readers = new BufferedReader[files.length];
        sks = new SortKey[files.length];
        index = new int[files.length];
        for (int i = 0; i < files.length; i++) {
            readers[i] = new BufferedReader(new FileReader(files[i]));
            index[i] = i;
            sks[i] = read(readers[i].readLine());
        }
        if (dsc) {
            for (int i = 0; i < files.length - 1; i++) {
                for (int j = i + 1; j < files.length; j++) {
                    if (sks[index[i]].compareTo(sks[index[j]]) > 0) {
                        int tmpIn = index[i];
                        index[i] = index[j];
                        index[j] = tmpIn;
                    }
                }
            }
        } else {
            for (int i = 0; i < files.length - 1; i++) {
                for (int j = i + 1; j < files.length; j++) {
                    if (sks[index[i]].compareTo(sks[index[j]]) < 0) {
                        int tmpIn = index[i];
                        index[i] = index[j];
                        index[j] = tmpIn;
                    }
                }
            }
        }
    }

    public SortKey read(String str) {
        String[] tmp = str.split("\\|");
        Record r = new Record(s);
        for (int j = 0; j < fs.size(); j++) {
            Field f = fs.get(j);
            switch (f.schema().getType()) {
                case INT:
                    r.put(j, Integer.parseInt(tmp[j]));
                    break;
                case LONG:
                    r.put(j, Long.parseLong(tmp[j]));
                    break;
                case FLOAT:
                    r.put(j, Float.parseFloat(tmp[j]));
                    break;
                case DOUBLE:
                    r.put(j, Double.parseDouble(tmp[j]));
                    break;
                case STRING:
                    r.put(j, tmp[j]);
                    break;
                default:
                    throw new ClassCastException("This type is not supported for Key type: " + f.schema());
            }
        }
        return new SortKey(r, keyNO);
    }

    public boolean hasNext() {
        return (start < files.length);
    }

    public void close() throws IOException {
        for (BufferedReader reader : readers) {
            reader.close();
        }
    }

    public SortKey next() throws IOException {
        SortKey r = sks[index[start]];
        String str = readers[index[start]].readLine();
        if (str == null) {
            start++;
        } else {
            sks[index[start]] = read(str);
            int m = start;
            if (dsc) {
                for (int i = start + 1; i < files.length; i++) {
                    if (sks[index[start]].compareTo(sks[index[i]]) > 0) {
                        m++;
                    } else {
                        break;
                    }
                }
            } else {
                for (int i = start + 1; i < files.length; i++) {
                    if (sks[index[start]].compareTo(sks[index[i]]) < 0) {
                        m++;
                    } else {
                        break;
                    }
                }
            }
            if (m > start) {
                int tmpIn = index[start];
                for (int i = start; i < m; i++) {
                    index[i] = index[i + 1];
                }
                index[m] = tmpIn;
            }
        }
        return r;
    }
}
