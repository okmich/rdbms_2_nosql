package com.okmich.hadoop.movielens.moviesummary;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingSummaryCombiner
        extends
        Reducer<LongWritable, SummaryRatingWritable, LongWritable, SummaryRatingWritable> {

    public RatingSummaryCombiner() {
    }

    /*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(
            LongWritable key,
            Iterable<SummaryRatingWritable> values,
            Reducer<LongWritable, SummaryRatingWritable, LongWritable, SummaryRatingWritable>.Context context)
            throws IOException, InterruptedException {

        // 1, [0.5.1.5,4.5]
        double rate = 0.0;
        int count = 0;

        long eTs = Long.MAX_VALUE;
        long lTs = Long.MIN_VALUE;

        for (SummaryRatingWritable dw : values) {
            rate += dw.getRate();
            count += dw.getCount();
            eTs = Math.min(eTs, dw.getEarliestRatingTs());
            lTs = Math.max(lTs, dw.getLatestRatingTs());
        }

        context.write(key, new SummaryRatingWritable(count, rate, eTs, lTs));

    }
}
