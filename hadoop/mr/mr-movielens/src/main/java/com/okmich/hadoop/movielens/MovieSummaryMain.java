package com.okmich.hadoop.movielens;

import com.okmich.hadoop.movielens.moviesummary.MovieRatingPartitioner;
import com.okmich.hadoop.movielens.moviesummary.RatingSummaryReducer;
import com.okmich.hadoop.movielens.moviesummary.SummaryRatingWritable;
import com.okmich.hadoop.movielens.moviesummary.RatingSummaryMapper;
import com.okmich.hadoop.movielens.moviesummary.RatingSummaryCombiner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovieSummaryMain extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Job job = new Job();
        job.setJobName("Movie Rating Summary");
        job.setJarByClass(MovieSummaryMain.class);

        // configure the map reduce job
        job.setMapperClass(RatingSummaryMapper.class);
        job.setReducerClass(RatingSummaryReducer.class);
        job.setCombinerClass(RatingSummaryCombiner.class);

        // set key and value class
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(SummaryRatingWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // io
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set partitioner
        job.setPartitionerClass(MovieRatingPartitioner.class);

        // no of reduce task
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new MovieSummaryMain(), args);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
