package com.okmich.sequencefile.converter;

import static org.apache.hadoop.io.SequenceFile.Writer.compression;
import static org.apache.hadoop.io.SequenceFile.Writer.file;
import static org.apache.hadoop.io.SequenceFile.Writer.keyClass;
import static org.apache.hadoop.io.SequenceFile.Writer.valueClass;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

public class SequenceFileSFPWriterWithCompression {

    public static void main(String[] args) throws Exception {
        String folder = "/home/cloudera/Downloads/t-drive-trajectory-data-sample/zips/test";

        File[] files = FileUtil.listFiles(new File(folder));
        Map<String, byte[]> map = new LinkedHashMap<>();
        int times = 0;
        for (File file : files) {
            map.put(file.getAbsolutePath(), Files.readAllBytes(file.toPath()));
            times++;
            if (times > 10) {
                break;
            }
        }

        //
        writeFile(map, folder + "/combined.seq");
    }

    /**
     *
     * @param docMap
     * @param fName
     * @throws IOException
     */
    public static void writeFile(Map<String, byte[]> docMap, String fName)
            throws Exception {
        SequenceFile.Writer writer = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);

            Path seqFilePath = new Path(fName);

            writer = SequenceFile.createWriter(conf, keyClass(Text.class),
                    valueClass(BytesWritable.class), file(seqFilePath),
                    compression(CompressionType.BLOCK, new DefaultCodec()));
            // write to the sequence file
            for (String fileName : docMap.keySet()) {
                writer.append(new Text(fileName),
                        new BytesWritable(docMap.get(fileName)));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }
}
