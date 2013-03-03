package cs492.PageRank.Preprocess;

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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;



public class BuildOutlinks {
	
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
//	private final static String wordSet = "2ne1;apple;batman;computer;doom2;europa;firefox;ghostbusters;hype;intharathit;jellyfish;kaist;lambada;metropolitan;nuclear;olympic;pizza;quasimodo;radiohead;slamdunk;twitter;umbrella;virtual;weezer;xylitol;ynglesdalen;zulu;";
//	private static ArrayList<String> selectedWords = null;

	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text>{
		
		@Override
		public void map(Text input, Text xmlChunk,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub

//			String singleXml = Text.decode(xmlChunk.getBytes());
			byte[] bb = xmlChunk.getBytes();
			String a = new String(bb, "UTF8");
//			String singleXml = xmlChunk.toString();
			String singleXml = a;
			
			//System.out.println("\n\nsingleXml :[" + singleXml +"]\n");
			
			int titleStart = singleXml.indexOf(TITLE_START_TAG);
			int titleEnd = singleXml.indexOf(TITLE_END_TAG);
			String title = singleXml.substring(titleStart + TITLE_START_TAG.length(), titleEnd);
			
			int idStart = singleXml.indexOf(ID_START_TAG, titleEnd);
			int idEnd = singleXml.indexOf(ID_END_TAG, idStart);
			String id = singleXml.substring(idStart + ID_START_TAG.length(), idEnd);
			
			System.out.println("\n\n\n\ntitle:[" + title +"]\n");
			
//			output.collect(new Text(title), new Text(title));
			
			int textStart = singleXml.indexOf(TEXT_START_TAG);
			int textEnd = singleXml.indexOf(TEXT_END_TAG);
			int textStartTagEnds = singleXml.indexOf(">", textStart);
			String text = "";
			
			if (textStartTagEnds + 1 > textEnd)
			{
				System.out.println("textstarttagends:"+ textStartTagEnds + " ; and textend:" + textEnd);
			}
			else
			{
				text = singleXml.substring(textStartTagEnds + 1, textEnd);
			}
			
			if (title.equals("Imperial Airways")) {
				System.out.println("\n\ntext :[" + text +"]\n");
			}
			
			ArrayList<String> linkList = new ArrayList<String>();
			int occStartIndex = text.indexOf(LINK_START_TAG);
			int occEndIndex = text.indexOf(LINK_END_TAG);
//			int occStartIndexLast = text.lastIndexOf(LINK_START_TAG);
//			int occEndIndexLast = text.lastIndexOf(LINK_END_TAG);
			
			try {
				
				int count = 0;
	//			System.out.println("text length = " + text.length());
				while (occStartIndex != -1) {// && occEndIndex != -1) {
					// TODO: |, #, Image: parsing
	//				System.out.println("occStart= " + occStartIndex + "  occEnd= " + occEndIndex);
					String nestCheck = text.substring(occStartIndex + 2, occEndIndex);
					if (nestCheck.contains(LINK_START_TAG)) {
						occStartIndex = text.lastIndexOf(LINK_START_TAG, occEndIndex);
						nestCheck = text.substring(occStartIndex + 2, occEndIndex);
					}
					
					if (nestCheck.contains("|")) {
						occEndIndex = text.indexOf("|", occStartIndex);
						nestCheck = text.substring(occStartIndex + 2, occEndIndex);
					}
					if (nestCheck.contains("#")) {
						occEndIndex = text.indexOf("#", occStartIndex);
						nestCheck = text.substring(occStartIndex + 2, occEndIndex);
					}
					
					linkList.add(nestCheck);
					System.out.println("nestCheck = " + nestCheck + " count = " + count++ + "start index = " + occStartIndex + "end index = " + occEndIndex);
					
					occStartIndex = text.indexOf(LINK_START_TAG, occStartIndex + 2);
					occEndIndex = text.indexOf(LINK_END_TAG, occStartIndex);					
				}
			}
			catch (Exception e) {
				System.out.println("linklist size = " + linkList.size());
				e.printStackTrace();
			}
//			title, id, PR, n of outlinks, outlinks

			String values = id + "\t" + Integer.toString(-1);	// initialize PR 
			values += "\t" + Integer.toString(linkList.size());
			for (String elem : linkList) {
				values += "\t" + elem;
			}
			output.collect(new Text(title), new Text(values));
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
		
		JobConf conf = new JobConf(BuildOutlinks.class);
		conf.setJobName("BuildOutlinks");
		
		conf.set("xmlinput.start", PAGE_START_TAG);
		conf.set("xmlinput.end", PAGE_END_TAG);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		 	
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
//		FileInputFormat.setInputPaths(conf, "/user/st01/mini_corpus/");
//		FileOutputFormat.setOutputPath(conf, new Path("/user/st01/output"));
		
//		MultipleOutputs.addMultiNamedOutput(conf, "asdf", TextOutputFormat.class, Text.class, Text.class);

		JobClient.runJob(conf);
	}
}
