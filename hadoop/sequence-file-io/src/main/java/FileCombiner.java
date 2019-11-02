import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author m.enudi
 */
public class FileCombiner {

	public static void main(String[] args) throws IOException {
		String folder = "/home/cloudera/Downloads/t-drive-trajectory-data-sample/zips/06";
		File file = new File(folder);
		File[] files = file.listFiles();

		List<String> contents = new ArrayList<String>();
		for (File f : files) {
			contents.addAll(Files.readAllLines(f.toPath(),
					Charset.defaultCharset()));
		}
		// sort
		Collections.sort(contents, new Comparator<String>() {

			public int compare(String o1, String o2) {
				return o1.split(",")[1].compareTo(o2.split(",")[1]);
			}

		});

		// write
		Files.write(new File(file, "combined.txt").toPath(), contents,
				Charset.defaultCharset(), StandardOpenOption.CREATE_NEW);
	}
}
