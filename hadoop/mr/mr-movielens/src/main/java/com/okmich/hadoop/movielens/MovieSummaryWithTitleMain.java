/**
 *
 */
package com.okmich.hadoop.movielens;

import com.okmich.hadoop.movielens.moviesummary2.MovieGroupComparator;
import com.okmich.hadoop.movielens.moviesummary2.MovieKey;
import com.okmich.hadoop.movielens.moviesummary2.MovieRatingWritable;
import com.okmich.hadoop.movielens.moviesummary2.MovieSortComparator;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author michael
 *
 */
public class MovieSummaryWithTitleMain extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(MovieSummaryWithTitleMain.class.getName());

    public MovieSummaryWithTitleMain() {
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
        job.setJarByClass(MovieSummaryWithTitleMain.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(MovieSummaryWithTitleReducer.class);

        job.setMapOutputKeyClass(MovieKey.class);
        job.setMapOutputValueClass(MovieRatingWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        // yes, I want 2 reducer
        job.setNumReduceTasks(2);

        job.setSortComparatorClass(MovieSortComparator.class);
        job.setGroupingComparatorClass(MovieGroupComparator.class);

        String[] args = new GenericOptionsParser(getConf(), arg0).getRemainingArgs();
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 1 : 0;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(new MovieSummaryWithTitleMain(), args);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     *
     * @author michael
     *
     */
    private static class RatingMapper extends Mapper<LongWritable, Text, MovieKey, MovieRatingWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("userId")) {
                return;
            }

            String[] texts = value.toString().split(",");
            int rating;
            try {
                rating = Integer.parseInt(texts[2]);
            } catch (NumberFormatException e) {
                rating = 0;
            }
            MovieKey movieKey = new MovieKey(MovieKey.CHILD);
            movieKey.set(new IntWritable(Integer.parseInt(texts[1])));

            MovieRatingWritable movieValue = new MovieRatingWritable();
            movieValue.setRating(new IntWritable(rating));
            movieValue.setMovieId(movieKey.getCode());

            context.write(movieKey, movieValue);
        }

    }

    /**
     *
     * @author michael
     *
     */
    private static class MovieMapper extends Mapper<LongWritable, Text, MovieKey, MovieRatingWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("movieId")) {
                return;
            }
            String[] texts = value.toString().split(",");

            MovieKey movieKey = new MovieKey(MovieKey.PARENT);
            movieKey.set(new IntWritable(Integer.parseInt(texts[0])));

            MovieRatingWritable movieValue = new MovieRatingWritable();
            movieValue.setRating(new IntWritable(-100));
            movieValue.setMovieId(new IntWritable(Integer.parseInt(texts[0])));
            if (texts.length == 3) {
                movieValue.setTitle(new Text(texts[1]));
            } else {
                movieValue.setTitle(new Text(texts[1] + ", " + texts[2]));
            }

            context.write(movieKey, movieValue);
        }
    }

    private static class MovieSummaryWithTitleReducer extends Reducer<MovieKey, MovieRatingWritable, NullWritable, Text> {

        @Override
        public void setup(Context context) {
            //retrieve a config value
            //int nVal = context.getConfiguration().getInt("n.value", 30);
        }

        @Override
        public void reduce(MovieKey key, Iterable<MovieRatingWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            double sum = 0.0;
            String movieTitle = null;
            for (MovieRatingWritable mrw : values) {
                if (-100 == mrw.getRating().get() || !mrw.getTitle().toString().isEmpty()) {
                    // this is  parent  record
                    movieTitle = mrw.getTitle().toString();
                } else {
                    ++count;
                    sum += mrw.getRating().get();
                }
            }

            double ave = count == 0 ? 0 : sum / count;
            String output = String.format("%-6d", key.getCode().get()) + String.format("%-60s", movieTitle)
                    + String.format("%12.2f", sum) + String.format("%5d", count) + String.format("%8.2f", ave);
            movieTitle = null;

            context.write(NullWritable.get(), new Text(output));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            //do nothing
        }
    }
}
