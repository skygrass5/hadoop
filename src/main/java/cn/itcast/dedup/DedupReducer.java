package cn.itcast.dedup;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
public class DedupReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable>values,Context context)throws IOException,
            InterruptedException{
        context.write(key, NullWritable.get());
    }
    //没有成功重写Reducer 因为类型不一样
//    protected void reduce(Text key, Iterable<NullWritable>values, Reducer.Context context)throws IOException,
//            InterruptedException{
//        context.write(key, NullWritable.get());
//    }
}
