/**
 *
 */
package com.okmich.hadoop.movielens.moviesummary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author cloudera
 *
 */
public class SummaryRatingWritable implements Writable {

    private long count;
    private double rate;
    private long earliestRatingTs;
    private long latestRatingTs;

    public SummaryRatingWritable() {
        this(0l, 0d, 0l, 0l);
    }

    public SummaryRatingWritable(long c, double r, long eTs, long lts) {
        this.count = c;
        this.rate = r;
        this.earliestRatingTs = eTs;
        this.latestRatingTs = lts;
    }

    /*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        out.writeDouble(rate);
        out.writeLong(getEarliestRatingTs());
        out.writeLong(getLatestRatingTs());
    }

    /*
	 * (non-Javadoc)
     */
    public void readFields(DataInput in) throws IOException {
        this.count = in.readLong();
        this.rate = in.readDouble();
        this.setEarliestRatingTs(in.readLong());
        this.setLatestRatingTs(in.readLong());
    }

    /**
     * @return the count
     */
    public long getCount() {
        return count;
    }

    /**
     * @return the rate
     */
    public double getRate() {
        return rate;
    }

    /**
     * @param count the count to set
     */
    public void setCount(long count) {
        this.count = count;
    }

    /**
     * @param rate the rate to set
     */
    public void setRate(double rate) {
        this.rate = rate;
    }

    /**
     * @return the earliestRatingTs
     */
    public long getEarliestRatingTs() {
        return earliestRatingTs;
    }

    /**
     * @param earliestRatingTs the earliestRatingTs to set
     */
    public void setEarliestRatingTs(long earliestRatingTs) {
        this.earliestRatingTs = earliestRatingTs;
    }

    /**
     * @return the latestRatingTs
     */
    public long getLatestRatingTs() {
        return latestRatingTs;
    }

    /**
     * @param latestRatingTs the latestRatingTs to set
     */
    public void setLatestRatingTs(long latestRatingTs) {
        this.latestRatingTs = latestRatingTs;
    }

}
