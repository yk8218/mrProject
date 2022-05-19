package seq;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@Log4j
public class CreateSequenceFile extends Configuration implements Tool {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            log.info("변환할 원본 파일과 시퀸스파일로 변환될 파일을 입려해야 합니다.");
            System.exit(-1);

        }
        int exitCode = ToolRunner.run(new CreateSequenceFile(), args);
        System.exit(exitCode);
    }
    @Override
    public void setConf(Configuration configuration) {

        configuration.set("AppName", "SequenceFile Create Test");

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

        log.info("appName : " + appName);

        Job job = Job.getInstance(conf);

        job.setJarByClass(CreateSequenceFile.class);

        job.setJobName(appName);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(0);

        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);

    }
}
