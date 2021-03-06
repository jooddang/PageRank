package cs492.PageRank.Iteration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;



public class Copy_2_of_PageRank {
	
	private final static String PAGE_START_TAG = "<page>";
	private final static String PAGE_END_TAG = "</page>";
	private final static String TITLE_START_TAG = "<title>";
	private final static String TITLE_END_TAG = "</title>";
	private final static String ID_START_TAG = "<id>";
	private final static String ID_END_TAG = "</id>";
	private final static String TEXT_START_TAG = "<text";
	private final static String TEXT_END_TAG = "</text>";
	private final static String LINK_START_TAG = "[[";
	private final static String LINK_END_TAG = "]]";
	private final static double DAMPING_FACTOR = 0.85;
	private final static double INITIAL_PR = 1.0 / 8951074.0;
//	private final static String wordSet = "2ne1;apple;batman;computer;doom2;europa;firefox;ghostbusters;hype;intharathit;jellyfish;kaist;lambada;metropolitan;nuclear;olympic;pizza;quasimodo;radiohead;slamdunk;twitter;umbrella;virtual;weezer;xylitol;ynglesdalen;zulu;";
//	private static ArrayList<String> selectedWords = null;

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		public void map(LongWritable input, Text outLinksLine,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			// singleLine : title, id, PR, n of outlinks, outlinks,
			byte[] bb = outLinksLine.getBytes();
			String singlePage = new String(bb, "UTF8");
//			String singlePage = outLinksLine.toString();
			
//			System.out.println("\n\nsinglePage :[" + singlePage +"]\n");
//			String[] splitTitle = singlePage.split("\t");
//			if (splitTitle.length != 2) {
//				System.out.println("ssibal fffail = [" + singlePage + "]");
//				return;
//			}
//			String[] ssibal = splitTitle[1].split("ssibal");
			String[] tempArray = singlePage.split("\t");
//			String[] tempArray = new String[ssibal.length + 1];
//			tempArray[0] = splitTitle[0];
//			for (int i = 0; i < ssibal.length; i++) {
//				tempArray[i + 1] = ssibal[i];
//			}
			
			if (tempArray.length < 5) {
				// 왠지 모르게 한 라인이 outlink에서 제대로 파싱 안되서... ㅠㅠ
//				System.out.println("ffffail = " + tempArray[0]);
				return;
			}
			
//			System.out.println("\n\n tempArray size :[" + tempArray.length +"]\n");
//			for (int i = 0; i < 4; i++) {
//				System.out.println("tempArray[" + i + "] = " + tempArray[i]);
//			}
			
			String title = tempArray[0];
			String id = tempArray[1];
			double pr;
			try {
				pr = Double.parseDouble(tempArray[2]);
				if (pr == -1) {
					/** TODO: 1 / page line
					 * */
					pr = INITIAL_PR;
				}
				
				int numOfOutlinks = Integer.parseInt(tempArray[3]);
			}
			catch (Exception e) {
//				System.out.println("double fffail = " + e.getMessage());
				return;
			}
//			System.out.println("PageRank = " + Double.toString(pr));
			
			ArrayList<String> outLinks = new ArrayList<String> ();
			for (int i = 4; i < tempArray.length; i++) {
				// 같은 페이지 중복 링크 제거
				if (outLinks.contains(tempArray[i]) == false) {
					outLinks.add(tempArray[i]);					
				}
			}

			System.gc();
			// 0 \t id \t numOfOutlinks \t outlinks
			// tempArray[3] 쓰는 이유는, 실제 계산시에는 중복 링크 제거해도 문서에는 중복된거 다 가지고 있어야 함. 그리고 그 number를 들고 있어야 함
			String outLinksString = "ToMyself" + "\t" + Integer.toString(outLinks.size()); 	// id & num of outlinks

			Iterator<String> it = outLinks.iterator();
			while (it.hasNext()) {
				String value = "ToOthers" + "\t" + Double.toString(pr / outLinks.size());
				String nextValue = it.next();
				outLinksString += "\t" + nextValue;
//				System.out.println("toOthers key[" + nextValue + "] value [" + value + "]");
				output.collect(new Text(nextValue), new Text(value));
			}
			
//			System.out.println("\n\n title : [" + title + "] outLinksString:[" + outLinksString + "]\n");
			
			output.collect(new Text(title), new Text(outLinksString));
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String outValues;
			ArrayList<String> outLinks = new ArrayList<String>();
			int onlyOne = 0;
//			String id = "";
			String numOfOutlinks = "";
			double pr = 0.0;
			
			while (iter.hasNext()) {
				String iterString = iter.next().toString();
				String[] values = iterString.split("\t");
				
//				System.out.println("rreducer iter [" + key.toString() + "] [" + iterString + "]");
//				for (String elem: values) {
//					System.out.println("rreducer iter elem [" + elem + "]");
//				}
				
				if (values[0].equals("ToMyself")) {
					//outlinks strings
					// 0 \t id \t numOfOutlinks \t outlinks
					if (onlyOne != 0) {
						// values[0] == 0 should be only once
//						output.collect(key,	new Text("reduce while fails; only one = " + Integer.toString(onlyOne)));
					}
					
					numOfOutlinks = values[1];
					for (int i = 2; i < values.length; i++) {
						outLinks.add(values[i]);
					}
					onlyOne++;
				}
				else if (values[0].equals("ToOthers")) {
					// PRs
//					System.out.println("tooooOthers");
					pr += Double.parseDouble(values[1]);
				}
				else {
//					output.collect(key, new Text("reduce while fails else..." + iterString));
				}
			}
//			if (onlyOne != 1) {
//				System.out.println("onlyOneee is = " + onlyOne);
//			}
			
			pr = (1.0 - DAMPING_FACTOR) + DAMPING_FACTOR * pr;
			
			if (key.toString().length() < 1 /*|| id.equals("")*/ || numOfOutlinks.equals("")) {
//				output.collect(key, new Text("reduce id / numofoutLinks fails; id = " + id));
//				output.collect(key, new Text("reduce id / numofoutLinks fails; numOfOutlinks = " + numOfOutlinks));
				return;
			}
			// singleLine : key(title), values (id, PR, n of outlinks, outlinks)
			outValues = Double.toString(pr) + "\t" + Integer.toString(outLinks.size());
			
			Iterator<String> it = outLinks.iterator();
			while (it.hasNext()) {
				outValues += "\t" + it.next().toString();
			}

//			System.out.println("\n\n Reducer outValues :[" + outValues + "]\n");
			
			output.collect(key, new Text(outValues));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
//		String singleXml = "asdf<text asldkfjiogv>11dkdldldlaa</text>asdf";
//
//		int textStart = singleXml.indexOf(TEXT_START_TAG);
//		int textEnd = singleXml.indexOf(TEXT_END_TAG);
//		int textStartTagEnds = singleXml.indexOf(">", textStart);
//		String text = singleXml.substring(textStartTagEnds + 1, textEnd);
//		
//		text = "a=b c[d]e:f;g(h)i{j}k,l.m|n/o*p'q\tr\rs\nt";
//
//		StringTokenizer st = new StringTokenizer(text, "= []:;(){},.|/*'\t\r\n");
//		
//		while (st.hasMoreTokens())
//		{
//			System.out.print(st.nextToken());
//		}
//		
//		Iterator<String> iter = (Iterator<String>) st;
//		String sum = new String();
//		while (iter.hasNext()){
//			sum += iter.next();
//			sum += ";";
//		}
//		System.out.print(sum);
		
		JobConf conf = new JobConf(Copy_2_of_PageRank.class);
		conf.setJobName("PageRank");
		
		conf.set("xmlinput.start", PAGE_START_TAG);
		conf.set("xmlinput.end", PAGE_END_TAG);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		 	
		conf.setMapperClass(Map.class);
//		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
//		FileInputFormat.setInputPaths(conf, "/user/st01/mini_corpus/");
//		FileOutputFormat.setOutputPath(conf, new Path("/user/st01/output"));

		JobClient.runJob(conf);
	}
}
