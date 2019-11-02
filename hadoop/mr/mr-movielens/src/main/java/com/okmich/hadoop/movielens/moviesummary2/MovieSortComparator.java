/**
 * 
 */
package com.okmich.hadoop.movielens.moviesummary2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author michael
 *
 */
public class MovieSortComparator extends WritableComparator {

	public MovieSortComparator() {
		super(MovieKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MovieKey one = (MovieKey) a;
		MovieKey other = (MovieKey) b;
		return one.compareTo(other);
	}
}