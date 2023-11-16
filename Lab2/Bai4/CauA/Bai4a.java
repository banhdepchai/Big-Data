import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai4a {

    public static class PriceCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final double THRESHOLD_PRICE = 100000.0;

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 4) {
            String date = fields[3].split(" ")[0]; // Lấy ngày từ ngày cập nhật
            try {
                double price = Double.parseDouble(fields[1]);
                if (price > THRESHOLD_PRICE) {
                    context.write(new Text(date), new LongWritable(1));
                }
            } catch (NumberFormatException e) {
                // Xử lý lỗi nếu dữ liệu không hợp lệ
                // Ví dụ: Log lỗi hoặc bỏ qua dòng không hợp lệ
            }
        }
        }
    }

    public static class PriceCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PriceCountMapReduce <input_path> <output_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Price Count");

        job.setJarByClass(Bai4a.class);
        job.setMapperClass(PriceCountMapper.class);
        job.setCombinerClass(PriceCountReducer.class);
        job.setReducerClass(PriceCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

