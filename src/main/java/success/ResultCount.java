package success;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


@Log4j
public class ResultCount extends Configuration implements Tool {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            log.info("분석할 폴더(파일) 및 분석결과가 저장될 폴더를 입력해야 합니다");
            System.exit(-1);

        }
        int exitCode = ToolRunner.run(new ResultCount(), args);
        System.exit(exitCode);
    }
    @Override
    public void setConf(Configuration configuration) {
        configuration.set("AppName", "Send Result");
        configuration.set("resultCode", "200");
    }
    @Override
    public Configuration getConf() {
        Configuration conf = new Configuration();

        this.setConf(conf);
        return conf;
    }
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        String appName = conf.get("AppName");

        log.info("appName : "+ appName);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ResultCount.class);
        job.setJobName(appName);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(ResultCountMapper.class);
        job.setReducerClass(ResultCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
    }
}
