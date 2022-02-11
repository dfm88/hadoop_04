package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
        ProductIdRatingWritable,    // Input value type
                Text,           // Output key type
        DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<ProductIdRatingWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        double rateSum = 0;
        int elQt = 0;
        // copy the iterable because ita has to be iterate 2 times (iterable get consumed)
        ArrayList<ProductIdRatingWritable> values2 = new ArrayList<>();
        // evaluate the rate average
        for(ProductIdRatingWritable el : values) {
            values2.add(el);
            rateSum = rateSum + el.getRating();
            elQt++;
        }
        double average = rateSum/elQt;

        // iterate once again to emit productId, rate-average
        // iterable 'values' has been consumed, iterate over his copy
        for(ProductIdRatingWritable el : values2) {
            context.write(new Text(key), new DoubleWritable(el.getRating()-average));
        }

    }
}
