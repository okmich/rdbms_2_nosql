package com.okmich.sequencefile.converter;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

import com.google.common.io.Files;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SequenceFileSFPReader {

    public static void main(String[] args) throws IOException {
        Map<String, byte[]> map = readFile(args[0]);

        for (String fileName : map.keySet()) {
            createAndWriteFile(fileName, map.get(fileName));
        }
    }

    public static Map<String, byte[]> readFile(String fName) throws IOException {
        SequenceFile.Reader reader = null;
        Configuration conf = new Configuration();
        Path seqFilePath = new Path(fName);
        try {
            reader = new SequenceFile.Reader(conf, Reader.file(seqFilePath));
            Text key = new Text();
            BytesWritable val = new BytesWritable();
            Map<String, byte[]> map = new LinkedHashMap<>();
            while (reader.next(key, val)) {
                map.put(key.toString(), val.getBytes());
                key = new Text();
                val = new BytesWritable();
            }
            return map;
        } catch (Exception ex) {
            Logger.getLogger("").log(Level.SEVERE, ex.getMessage(), ex);
            throw ex;
        } finally {
            reader.close();
        }
    }

    public static void createAndWriteFile(String fileName, byte[] content)
            throws IOException {
        File file = new File(fileName);
        Files.write(content, file);
    }
}
