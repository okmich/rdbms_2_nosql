package com.okmich.hadoop.movielens.moviesummary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MovieRatingPartitioner extends
        Partitioner<LongWritable, SummaryRatingWritable> {

    public MovieRatingPartitioner() {
    }

    @Override
    public int getPartition(LongWritable key, SummaryRatingWritable value,
            int partitions) {
        // 0 - 45000
        long id = key.get();
        if (id >= 0 && (id < 10000)) {
            return 0;
        } else if (id >= 10000 && id < 20000) {
            return 1;
        } else if (id >= 20000 && id < 30000) {
            return 2;
        } else if (id >= 30000 && id < 40000) {
            return 3;
        } else {
            return 4;
        }
    }
}
