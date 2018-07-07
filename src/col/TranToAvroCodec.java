/**
 * 
 */
package col;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;

import cores.avro.FilterBatchColumnReader;

/**
 * @author iclab
 *
 */
public class TranToAvroCodec {
    public static void main(String[] args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        File fromFile = new File(args[1]);
        File toFile = new File(args[2]);
        int max = Integer.parseInt(args[3]);
        DatumWriter<Record> writer = new GenericDatumWriter<Record>(s);
        DataFileWriter<Record> fileWriter = new DataFileWriter<Record>(writer);
        if (!toFile.getParentFile().exists()) {
            toFile.getParentFile().mkdirs();
        }
        switch (args[4]) {
            case "bzip2":
                fileWriter.setCodec(CodecFactory.bzip2Codec());
                break;
            case "snopy":
                fileWriter.setCodec(CodecFactory.snappyCodec());
                break;
            case "null":
                fileWriter.setCodec(CodecFactory.nullCodec());
                break;
            case "deflate":
                fileWriter.setCodec(CodecFactory.deflateCodec(Integer.parseInt(args[5])));
                break;
            case "xz":
                fileWriter.setCodec(CodecFactory.xzCodec(Integer.parseInt(args[5])));
                break;
            default:
                break;
        }
        fileWriter.create(s, toFile);

        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(fromFile);
        reader.createSchema(s);
        //        long t1 = System.currentTimeMillis();
        //        reader.filterNoCasc();
        //        long t2 = System.currentTimeMillis();
        reader.createRead(max);

        while (reader.hasNext()) {
            Record r = reader.next();
            fileWriter.append(r);
        }
        reader.close();
        fileWriter.close();
    }
}
