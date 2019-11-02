/**
 *
 */
package com.okmich.hadoop.movielens.moviesummary2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * @author michael
 */
public class MovieGroupComparator extends WritableComparator {

    public MovieGroupComparator() {
        super(MovieKey.class, true);
    }

    /*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.
	 * WritableComparable, org.apache.hadoop.io.WritableComparable)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MovieKey one = (MovieKey) a;
        MovieKey other = (MovieKey) b;

        return one.getCode().compareTo(other.getCode());
    }

}
