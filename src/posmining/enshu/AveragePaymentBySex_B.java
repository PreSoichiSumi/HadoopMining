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
 * 種類別のおにぎりの個数
 * @author 2015020 賀数
 *
 */
public class AveragePaymentBySex_B {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(OnigiriCountByType.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2015020");                   // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		// 入出力ファイルを指定
		String inputpath = "posdata";
		String outputpath = "out/paymentBySexB";     // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(8);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}


	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			//keyとなる日付を取得
			String sex = csv[PosUtils.BUYER_SEX];
			if(sex.startsWith("1")){
				sex = "男";
			}else if(sex.startsWith("2")){
				sex = "女";
			}

			// valueとなる売り上げを取得
			String price = csv[PosUtils.ITEM_TOTAL_PRICE];

			String rIdAndPrice = csv[PosUtils.RECEIPT_ID] + ":" + price;

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(sex), new CSKV(rIdAndPrice));
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			// 売り上げを合計
			int total_price = 0;
			String lastId = "";
			int onePrice = 0;
			int count =0;
			String rId="";
			int tanka=0;
			for (CSKV value : values) {
				//System.out.println("test" + value.toString());
				String c[] = value.toString().split(":");
				rId = c[0];
				tanka = Integer.parseInt(c[1]);

				if(rId == lastId){
					onePrice += tanka;
				}else{
					total_price += onePrice;
					lastId = rId;
					count++;
					onePrice = tanka;
				}
			}
			total_price += onePrice;
			//count++;
			onePrice = tanka;

			System.out.println(total_price + " , " + count);

			double ave_price = (double)total_price/count;

			// emit
			context.write(key, new CSKV(ave_price));
		}
	}
}
