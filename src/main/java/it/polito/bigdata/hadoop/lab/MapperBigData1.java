package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    ProductIdRatingWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the map method */

        // File header:
        //         1      2                                                              6
        // id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,Score,Time,Summary,Text

        String[] row = value.toString().split(",");

        String prodId = row[0];
        String userId = row[1];

        // Excluding the header row
        if(!prodId.equals("Id")){
            double rate = Double.parseDouble(row[6]);
            context.write(new Text(userId), new ProductIdRatingWritable(prodId, rate));
        }

    }
}
