import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai4c {

    public static class PriceMapper extends Mapper<Object, Text, Text, Text> {
        private Text product = new Text();
        private Text priceInfo = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 4) {
                product.set(fields[0]);
                priceInfo.set(fields[1]);
                context.write(product, priceInfo);
            }
        }
    }

    public static class PriceReducer extends Reducer<Text, Text, Text, Text> {
        private Text maxPrice = new Text();
        private Text minPrice = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;

            for (Text value : values) {
                double price = Double.parseDouble(value.toString());
                if (price > max) {
                    max = price;
                }
                if (price < min) {
                    min = price;
                }
            }

            maxPrice.set("Max Price: " + max);
            minPrice.set("Min Price: " + min);

            context.write(key, new Text(maxPrice.toString() + ", " + minPrice.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(Bai4c.class);
        job.setMapperClass(PriceMapper.class);
        job.setReducerClass(PriceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

