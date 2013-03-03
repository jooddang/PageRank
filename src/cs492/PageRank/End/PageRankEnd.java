package cs492.PageRank.End;

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



public class PageRankEnd {
	
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
//			byte[] bb = outLinksLine.getBytes();
//			String singlePage = new String(bb, "UTF8");
			String singlePage = outLinksLine.toString();
			
			if (singlePage.contains("\t") == false) {
				return;
			}
			
			StringTokenizer st = new StringTokenizer(singlePage, "\t");

//			System.out.println("singlePage = [" + singlePage + "] ");
			
			
			if (st.countTokens() < 2) {
				// 왠지 모르게 한 라인이 outlink에서 제대로 파싱 안되서... ㅠㅠ
//				System.out.println("ffffail = " + tempArray[0]);
				return;
			}
			
//			System.out.println("\n\n tempArray size :[" + tempArray.length +"]\n");
//			for (int i = 0; i < 4; i++) {
//				System.out.println("tempArray[" + i + "] = " + tempArray[i]);
//			}
			
			String title = st.nextToken();
			double pr;
			try {
				pr = Double.parseDouble(st.nextToken());
				if (pr == -1) {
					/** TODO: 1 / page line
					 * */
					pr = INITIAL_PR;
				}
				
//				int numOfOutlinks = Integer.parseInt(st.nextToken());
			}
			catch (Exception e) {
//				System.out.println("double fffail = " + e.getMessage());
				return;
			}
			
			Text keyForOthers = new Text();
			Text valueForOthers = new Text();

			keyForOthers.set(title);
			valueForOthers.set(Double.toString(pr));
			output.collect(keyForOthers, valueForOthers);
		}
	}
	
	public static class Reduce extends IdentityReducer<Text, Text> {	
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
		
		JobConf conf = new JobConf(PageRankEnd.class);
		conf.setJobName("PageRankEnd");
		
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
