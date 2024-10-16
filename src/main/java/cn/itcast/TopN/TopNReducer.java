package cn.itcast.TopN;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, IntWritable,
        NullWritable, IntWritable> {
    private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer,
            String>(new Comparator<Integer>() {
        public int compare(Integer o1, Integer o2) {
//            return o2 - o1; //倒序
            return o1 - o2;  //正序

           /*返回正数表示：o1小于o2
             返回0表示：o1和o2相等
             返回负数表示：01大于o2
            */
        }

    });

    public void reduce(NullWritable key, Iterable<IntWritable>values,
                       Context context)throws IOException, InterruptedException{
        for (IntWritable value : values){
            repToRecordMap.put(value.get(), " ");
            //如果map和reduce不同的话
            if (repToRecordMap.size() > 5) {
//                repToRecordMap.remove(repToRecordMap.firstKey()); //正序得到最大的数 倒序得到最小的数
                repToRecordMap.remove(repToRecordMap.lastKey()); //倒序得到最大的数 正序得到最小的数
            }
        }
//        System.out.println("reduce:"+repToRecordMap.keySet());

        for (Integer i : repToRecordMap.keySet()){
            context.write(NullWritable.get(), new IntWritable(i));
        }
    }

}
