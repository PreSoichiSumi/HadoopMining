package posmining.enshu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import posmining.utils.CSKV;
import posmining.utils.PosUtils;

/**
 * @author 2015004 鷲見
 */
public class PrepareData {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(PrepareData.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2015004");

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		// 入出力ファイルを指定
		String inputpath = "posdata";
		String outputpath = "out/DatasForEachPlace";     // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(16);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}

	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			// valueとなる売り上げを取得
			String code = csv[PosUtils.ITEM_CATEGORY_CODE];
			if(Integer.parseInt(code)!=163005)
				return;
			String place;
			String test=csv[PosUtils.LOCATION];
			if(test.startsWith("オフィス")){
				place="オフィス街";
			}else if(test.startsWith("ロード")){
				place="ロードサイド";
			}else if(test.startsWith("住宅")){
				place="住宅街";
			}else if(test.startsWith("駅前")){
				place="駅前";
			}else{
				throw new IOException("(汗");
			}
			
			
			String data=csv[PosUtils.MONTH]+","+csv[PosUtils.WEEK]+","+csv[PosUtils.HOUR]+","+csv[PosUtils.ITEM_NAME]+","+csv[PosUtils.ITEM_COUNT]
					+","+csv[PosUtils.ITEM_PRICE];
			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(place), new CSKV(data));
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			// 売り上げを合計
			//Integer count=0;
			// emit
			for(CSKV o: values){
				context.write(key, o);
			}
		}
	}
}
