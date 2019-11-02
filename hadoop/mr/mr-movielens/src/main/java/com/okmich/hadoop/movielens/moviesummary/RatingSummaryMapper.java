package com.okmich.hadoop.movielens.moviesummary;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingSummaryMapper extends
        Mapper<LongWritable, Text, LongWritable, SummaryRatingWritable> {

    public RatingSummaryMapper() {
    }

    /*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(
            LongWritable key, // file offset
            Text value, // line of text
            Mapper<LongWritable, Text, LongWritable, SummaryRatingWritable>.Context context)
            throws IOException, InterruptedException {
        LongWritable outputKey = new LongWritable();
        SummaryRatingWritable outputValue = new SummaryRatingWritable();

        // userId,movieId,rating,timestamp
        // 1,110,1.0,1425941529
        String record = value.toString();
        if (record.startsWith("userId")) {
            return;
        }

        String[] splits = record.split(",");
        long movieId = Long.parseLong(splits[1]);
        double rating = Double.parseDouble(splits[2]);
        long ts = Long.parseLong(splits[3]);

        outputKey.set(movieId);
        outputValue.setCount(1);
        outputValue.setRate(rating);
        outputValue.setEarliestRatingTs(ts);
        outputValue.setLatestRatingTs(ts);

        // select movieid, rating, ts from movielen table????
        context.write(outputKey, outputValue);
    }
}
