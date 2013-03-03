package cs492.PageRank.Preprocess;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;


// reference from http://mail-archives.apache.org/mod_mbox/hadoop-core-user/200803.mbox/%3C47CC9221.9070903@metaweb.com%3E
public class XmlInputFormat extends TextInputFormat{
	//public static final String START_TAG_KEY = "xmlinput.start";
    //public static final String END_TAG_KEY = "xmlinput.end";

    public void configure(JobConf jobConf) {
        super.configure(jobConf);
    }

    public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
throws IOException {
        return new XmlRecordReader((FileSplit) inputSplit, jobConf);
    }

    public static class XmlRecordReader implements RecordReader {
        private byte[] startTag;
        private byte[] endTag;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();

        public XmlRecordReader(FileSplit split, JobConf jobConf) throws IOException {
            startTag = jobConf.get("xmlinput.start").getBytes("utf-8");
            endTag = jobConf.get("xmlinput.end").getBytes("utf-8");

            // open the file and seek to the start of the split
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(jobConf);
            fsin = fs.open(split.getPath());
            fsin.seek(start);
        }

		@Override
		public boolean next(Object key, Object value) throws IOException {
        //public boolean next(WritableComparable key, Writable value) throws IOException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            ((Text) key).set(Long.toString(fsin.getPos()));
                            ((Text) value).set(buffer.getData(), 0, buffer.getLength());
                            return true;
                        }
                    }
                    finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        public WritableComparable createKey() {
            return new Text();
        }

        public Writable createValue() {
            return new Text();
        }

        public long getPos() throws IOException {
            return fsin.getPos();
        }

        public void close() throws IOException {
            fsin.close();
        }

        public float getProgress() throws IOException {
            return ((float) (fsin.getPos() - start)) / ((float) (end - start));
        }

        /////////////////////////////////////////////////

        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException
        {
            int i = 0;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) 
                	return false;
                // save to buffer:
                if (withinBlock) 
                	buffer.write(b);

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) 
                    	return true;
                } 
                else 
                	i = 0;
                // see if we've passed the stop point:
                if(!withinBlock && i == 0 && fsin.getPos() >= end) 
                	return false;
            }
        }

    }
}
