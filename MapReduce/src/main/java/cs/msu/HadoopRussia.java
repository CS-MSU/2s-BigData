package cs.msu;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

import static cs.msu.SequenceFileUtils.toSequenceString;
import static cs.msu.SequenceFileUtils.fromSequenceString;


/**
 * Соединяет полученные таблицы с пользователями из России с их ответами и подсчитывает рейтинг каждого
 */

public class HadoopRussia extends Configured implements Tool {

    private static class AnswersMapper extends Mapper<Object, Text, LongWritable, Text> {
        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();
        private final String[] input_fields = new String[] {
                "OwnerUserId", "Score"
        };
        private final String[] output_fields = new String[] {
                "Id", "Type", "Score", "DisplayName", "Location"
        };

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = fromSequenceString(value, input_fields);

            if(StringUtils.isBlank(row.get("OwnerUserId"))) return;

            row.put("Type", "answer");
            outValue.set(toSequenceString(row, output_fields));
            outKey.set(Long.parseLong(row.get("OwnerUserId")));
            context.write(outKey, outValue);
        }
    }

    private static class UsersMapper extends Mapper<Object, Text, LongWritable, Text> {
        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();

        private final String[] input_fields = new String[] {
                "Id", "DisplayName", "Location"
        };
        private final String[] output_fields = new String[] {
                "Id", "Type", "Score", "DisplayName", "Location"
        };

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = fromSequenceString(value, input_fields);

            row.put("Type", "user");
            outValue.set(toSequenceString(row, output_fields));
            outKey.set(Long.parseLong(row.get("Id")));
            context.write(outKey, outValue);
        }
    }

    static class JoinReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
        private final String[] input_fields = new String[] {
                "Id", "Type", "Score", "DisplayName", "Location"
        };
        private final String[] output_fields = new String[] {
                "Id", "SumScore", "DisplayName", "Location"
        };
        private Text outValue = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long SumScore = 0;
            boolean hasAnswers = false;
            Map<String, String> user = null;

            for (Text value : values) {
                Map<String, String> row = fromSequenceString(value, input_fields);
                if(row.get("Type").equals("answer")) {
                    hasAnswers = true;
                    SumScore += Long.parseLong(row.get("Score"));
                } else {
                    user = row;
                }
            }

            if (hasAnswers && user != null) {
                user.remove("Type");
                user.put("SumScore", Long.toString(SumScore));
                outValue.set(toSequenceString(user, output_fields));
                context.write(NullWritable.get(), outValue);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Path answersPath = new Path(args[0]);
        Path usersPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(getConf(), "Join Posts and Users");
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(HadoopRussia.class);
        // Указываем класс Редьюсера
        job.setReducerClass(JoinReducer.class);
        // Кол-во тасков
        job.setNumReduceTasks(10);
        // Тип ключа на выходе
        job.setOutputKeyClass(NullWritable.class);
        // Тип значения на выходе
        job.setOutputValueClass(Text.class);
        // Пути к входным файлам, формат файла и мэппер
        MultipleInputs.addInputPath(job, answersPath, SequenceFileInputFormat.class, AnswersMapper.class);
        MultipleInputs.addInputPath(job, usersPath, SequenceFileInputFormat.class, UsersMapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // Включаем компрессию
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        // Возвращаем ее статус в виде exit-кода процесса
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int result = ToolRunner.run(new Configuration(), new HadoopRussia(), args);

        System.exit(result);
    }

}