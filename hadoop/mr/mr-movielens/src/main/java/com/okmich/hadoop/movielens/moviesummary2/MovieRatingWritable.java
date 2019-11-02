/**
 * 
 */
package com.okmich.hadoop.movielens.moviesummary2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author michael
 *
 */
public class MovieRatingWritable implements Writable {

	private IntWritable movieId = new IntWritable();
	private Text title = new Text();
	private IntWritable rating = new IntWritable();

	public MovieRatingWritable() {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.movieId.readFields(in);
		this.title.readFields(in);
		this.rating.readFields(in);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.movieId.write(out);
		this.title.write(out);
		this.rating.write(out);
	}

	/**
	 * @return the movieId
	 */
	public IntWritable getMovieId() {
		return movieId;
	}

	/**
	 * @param movieId
	 *            the movieId to set
	 */
	public void setMovieId(IntWritable movieId) {
		this.movieId = movieId;
	}

	/**
	 * @return the title
	 */
	public Text getTitle() {
		return title;
	}

	/**
	 * @param title
	 *            the title to set
	 */
	public void setTitle(Text title) {
		this.title = title;
	}

	/**
	 * @return the rating
	 */
	public IntWritable getRating() {
		return rating;
	}

	/**
	 * @param rating
	 *            the rating to set
	 */
	public void setRating(IntWritable rating) {
		this.rating = rating;
	}

	@Override
	public String toString() {
		return "Rating[movieId:" + this.movieId.get() + ",title:" + this.title.toString()
		+ ",rating:" + this.rating.get() + "]";
	}
}
