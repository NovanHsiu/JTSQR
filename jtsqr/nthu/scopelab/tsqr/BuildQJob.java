/**
 * Do Q = Q1 x Q2 x ... x Qk multiplicaiton
 * The final Q multiplied by mutiple first Q
--------------
 Input: 
	first Q matrix
		<key, value> : <Id of submatrix, first Q submatrix>
 Output: 
	final Q matrix
		<key, value> : <Id of submatrix, final Q submatrix>
**/
package nthu.scopelab.tsqr;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.lang.Math;
import java.security.InvalidAlgorithmParameterException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.SequenceFile;

import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.MatrixEntry;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import no.uib.cipr.matrix.sparse.FlexCompColMatrix;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.DenseVector;

import nthu.scopelab.tsqr.matrix.MatrixWritable;
import nthu.scopelab.tsqr.matrix.SparseRowBlockAccumulator;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.TSQRunner;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.TSQRunner.fileGather;
import nthu.scopelab.tsqr.TSQRunner.Checker;

@SuppressWarnings("unchecked")
public class BuildQJob extends TSQRunner{
  private static final boolean debug = false;
  public static final String QRF_DIR = "qrf.dir";
  public static final String SCHEDULE_NUM = "schedule.number";
  
  public enum BuildQJobTime { Computation, Total }
  private BuildQJob() {
  }
  
  public static void run(Configuration conf,String inputPathStr,String outputPathStr,
				String reduceSchedule,int mis) throws Exception {
    boolean outputQ = true;
	
    	
    String stages[] = reduceSchedule.split(",");
	String qrfpath = inputPathStr + "/iter-r-1";
	Path outputPath = new Path(outputPathStr);
	
	JobConf job = new JobConf(conf, BuildQJob.class);
	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	job.setInt(SCHEDULE_NUM,stages.length);
	job.set(QRF_DIR,inputPathStr);
	FileSystem.get(job).delete(outputPath, true);		
    
	FileOutputFormat.setOutputPath(job, outputPath);
	job.setJobName("BuildQJob");
	
	job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MatrixWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(MatrixWritable.class);	
	job.setMapperClass(BuildQMapper.class);
	
	FileSystem fs = FileSystem.get(job);
	Path Paths[];
	fileGather fgather = new fileGather(new Path(qrfpath),QRFirstJob.QF_MAT+"-m-",fs);
	Paths = fgather.getPaths();
	mis = Checker.checkMis(mis,fgather.getInputSize(),fs);
	job.setNumMapTasks(fgather.recNumMapTasks(mis));
	
    job.setNumReduceTasks(0);
	
	FileInputFormat.setInputPaths(job,Paths);
	
	 RunningJob rj = JobClient.runJob(job);
	 System.out.println("BuildQJob Job ID: "+rj.getJobID().toString());
	 System.out.println("BuildQJob Computation Time: "+rj.getCounters().findCounter(BuildQJobTime.Computation).getValue()+
	 ", BuildQJob Total Time: "+rj.getCounters().findCounter(BuildQJobTime.Total).getValue());
    }
	
	public static class BuildQMethod extends MapReduceBase
    {
	 protected String qrfpath, filename; 
	 protected int Q_TaskId = -1; //Task Id of first QR Factorization Map Task
	 protected FileSystem fs;
	 protected int oiter;
	 protected int scheduleNumber;
	 protected int taskId;	
	 protected cmDenseMatrix Q = null;
	 private List<cmDenseMatrix> QList = new ArrayList<cmDenseMatrix>();
	 private List<Map<Integer,QIndexPair>> QIndexMapList = new ArrayList<Map<Integer,QIndexPair>>();
	 private IntWritable key = null;
	 private MatrixWritable value = null;
	@Override
	public void configure(JobConf job)
	{
		filename = job.get("map.input.file");
		filename = filename.substring(filename.lastIndexOf("/")+1);
		if(filename.split("-").length>2)
		 Q_TaskId = Integer.valueOf(filename.split("-")[2]);
		
		 taskId = Integer.parseInt(job.get("mapred.task.id").split("_")[4]);
		 try{
		 qrfpath = job.get(QRF_DIR);
		 scheduleNumber = job.getInt(SCHEDULE_NUM,1);
		 fs = FileSystem.get(job);	
		  //build Index Map: 13/12/24
		 SequenceFile.Reader reader = null;
		 oiter = 1;
		 while(fs.exists(new Path(qrfpath+"/iter-r-"+Integer.toString(oiter))))
		 {
			Map<Integer,QIndexPair> QIndexMap = new HashMap<Integer,QIndexPair>();
		    FileStatus[] lastQStatus = fs.listStatus(new Path(qrfpath+"/iter-r-"+Integer.toString(oiter)), new QRFactorMultiply.MyPathFilter(QRFirstJob.QF_MAT+"-r-"));
			for(int i=0;i<lastQStatus.length;i++)
			{
		    reader = new SequenceFile.Reader(fs, lastQStatus[i].getPath(), fs.getConf());			
		    IntWritable lkey = (IntWritable) reader.getKeyClass().newInstance();
			long offset = reader.getPosition();
		    while(reader.next(lkey))
			{
			 QIndexMap.put(lkey.get(),new QIndexPair(lastQStatus[i].getPath(),offset));
			 offset = reader.getPosition();
			}
			QIndexMapList.add(QIndexMap);
			}
			oiter++;
		 }
		}
		catch(Exception e)
		{
		 e.printStackTrace();
		 throw new NullPointerException("Exception!");
		}		 
	}
	
	public void BuildQ(IntWritable key, MatrixWritable value) throws IOException
	{
		//read last sub Q
		int preTaskId = Q_TaskId;
		QList.clear();
		SequenceFile.Reader reader = null;
		try{
			 for(int i=0;i<QIndexMapList.size();i++)
			 {
			 QIndexPair qip = QIndexMapList.get(i).get(preTaskId);
			 reader = new SequenceFile.Reader(fs, qip.path, fs.getConf());			
		     IntWritable lkey = (IntWritable) reader.getKeyClass().newInstance();
		     MatrixWritable lvalue = (MatrixWritable) reader.getValueClass().newInstance();
			 reader.seek(qip.offset);
		     reader.next(lkey,lvalue);  
		     QList.add(lvalue.getDense().copy());
			 preTaskId = Integer.valueOf(qip.path.getName().split("-")[2]);
			 }
			reader.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			throw new NullPointerException("Error!");
		}			

			int m = value.matNumRows();
			
			if(Q==null)
			{
			 Q = new cmDenseMatrix(new double[value.getDense().numRows()*value.getDense().numColumns()*2],value.getDense().numRows(),value.getDense().numColumns());
			}
			else
			{
			 Q.set(Q.getData(),value.getDense().numRows(),value.getDense().numColumns());
			}
			cmDenseMatrix firstQ = value.getDense();
			while(!QList.isEmpty())
			{
		 	 Q = QRFactorMultiply.Multiply("N","N",firstQ.copy(),QList.remove(0),Q); 
			 firstQ = Q;
			}
	}
			
   }
	
	public static class BuildQMapper //only work on iteration 1 (index 0)
        extends BuildQMethod implements Mapper<IntWritable, MatrixWritable, IntWritable, MatrixWritable>{
		private long start, end;
		private long confstart, mapstart, time1, time2, mapAcc = 0;
		private long computationTime = 0, tag0, tag1;
		private Reporter reporter;
		
		@Override
		public void configure(JobConf job)
		{
		 confstart = new Date().getTime();
		 tag0 = confstart;
		 super.configure(job);
		 time1 = new Date().getTime();
		 if(debug)
		 System.out.println("map_conf: "+(time1-confstart));
		 tag1 = time1;
		 computationTime+=tag1-tag0;
		}
		
        public void map(IntWritable key, MatrixWritable value, OutputCollector<IntWritable, MatrixWritable> output, Reporter reporter)
            throws IOException {
			this.reporter = reporter;
			tag0 = new Date().getTime();
			BuildQ(key,value);
			value.set(Q);
			output.collect(key,value);
			tag1 = new Date().getTime();
			computationTime+=tag1-tag0;
        }
		
		@Override
		public void close() throws IOException {
		time2 = new Date().getTime();
		if(taskId==0)
		{
			reporter.incrCounter(BuildQJobTime.Computation,computationTime);
			reporter.incrCounter(BuildQJobTime.Total,time2-confstart);
		}
		}
		
    }
	
	public static class QIndexPair{
	
	public Path path;
	public long offset;
	
	public QIndexPair(Path path,long offset)
	{
	 this.path = path;
	 this.offset = offset;
	}	
	}
}
