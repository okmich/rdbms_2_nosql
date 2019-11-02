/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.hadoop.movielens;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author michael
 *
 */
public class GenreCountingMain extends Configured implements Tool {

    /**
     * RECORD_DELIMITER
     */
    private static final String RECORD_DELIMITER = ",";

    /**
     * GENRE_DELIMITER
     */
    private static final String GENRE_DELIMITER = "\\|";

    /**
     *
     */
    public GenreCountingMain() {
        // TODO Auto-generated constructor stub
    }

    /**
     * @param conf
     */
    public GenreCountingMain(Configuration conf) {
        super(conf);
        // TODO Auto-generated constructor stub
    }

    /*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] arg0) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(GenreCountingMain.class);

        job.setMapperClass(GenreCountingMapper.class);
        job.setCombinerClass(GenreCountingReducer.class);
        job.setReducerClass(GenreCountingReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String[] args = new GenericOptionsParser(getConf(), arg0).getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 1 : 0;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        @SuppressWarnings("unused")
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(new GenreCountingMain(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     *
     * @author michael
     *
     */
    public static class GenreCountingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        /**
         * @param key
         * @param value
         * @param context
         * @throws InterruptedException
         * @throws IOException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("movieId")) {
                return;
            }
            String[] fields = value.toString().split(RECORD_DELIMITER);
            String genresPart = fields[fields.length - 1];

            String[] genres = genresPart.split(GENRE_DELIMITER);
            for (String genre : genres) {
                //Drama, 1
                context.write(new Text(genre), new IntWritable(1));
            }
        }
    }

    /**
     *
     * @author michael
     *
     */
    public static class GenreCountingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         *
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // write the count for each to the stream
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            context.write(new Text(String.format("%-30s", key.toString())), new IntWritable(count));
        }
    }

}
