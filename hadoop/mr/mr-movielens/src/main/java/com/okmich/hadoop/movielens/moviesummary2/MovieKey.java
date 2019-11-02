/**
 * 
 */
package com.okmich.hadoop.movielens.moviesummary2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author michael
 *
 */
public class MovieKey implements WritableComparable<MovieKey> {

	public static final IntWritable PARENT = new IntWritable(0);
	public static final IntWritable CHILD = new IntWritable(1);
	private IntWritable type = new IntWritable();
	private IntWritable code = new IntWritable();

	public MovieKey() {
		super();
	}

	public MovieKey(IntWritable type) {
		this();
		this.type = type;
	}

	public void set(IntWritable code) {
		this.code = code;
	}

	/**
	 * @return the type
	 */
	public IntWritable getType() {
		return type;
	}

	/**
	 * @return the code
	 */
	public IntWritable getCode() {
		return code;
	}

	@Override
	public void readFields(DataInput din) throws IOException {
		this.type.readFields(din);
		this.code.readFields(din);
	}

	@Override
	public void write(DataOutput dout) throws IOException {
		this.type.write(dout);
		this.code.write(dout);
	}

	@Override
	public int hashCode() {
		return this.code.hashCode();
	}

	@Override
	public int compareTo(MovieKey other) {
		if (this.code.compareTo(other.code) == 0) {
			return this.type.compareTo(other.type);
		} else {
			return this.code.compareTo(other.code);
		}
	}

	@Override
	public String toString() {
		return "MovieKey[type:" + this.type.get() + ",code:" + this.code.toString() + "]";
	}
}
