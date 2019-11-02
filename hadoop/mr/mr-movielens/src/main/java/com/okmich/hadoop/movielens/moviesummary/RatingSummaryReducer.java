package com.okmich.hadoop.movielens.moviesummary;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingSummaryReducer extends
        Reducer<LongWritable, SummaryRatingWritable, NullWritable, Text> {

    private final static DecimalFormat DF = new DecimalFormat("0.00");

    public RatingSummaryReducer() {
    }

    /*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(LongWritable key,
            Iterable<SummaryRatingWritable> values,
            Reducer<LongWritable, SummaryRatingWritable, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        Text outputText = new Text();
        // 1, [0.5.1.5,4.5]
        long movieId = key.get();
        double rate = 0.0;
        int count = 0;
        long earliesTs = Long.MAX_VALUE;
        long latestTs = Long.MIN_VALUE;

        for (SummaryRatingWritable dw : values) {
            rate += dw.getRate();
            count += dw.getCount();
            earliesTs = Math.min(earliesTs, dw.getEarliestRatingTs());
            latestTs = Math.max(latestTs, dw.getLatestRatingTs());
        }

        double average = rate / count;

        outputText.set(movieId + "\t" + count + "\t"
                + rate + "\t" + DF.format(average) + "\t" + earliesTs + "\t" + latestTs);

        context.write(NullWritable.get(), outputText);

    }
}
