/**
 * Merge sub matrix and do tall and skinny QR (TSQR) factorization.
--------------
 Input: 
	Sub matrices of A
		<key, value> : <Id of submatrix, submatrix>
 Output: 
	1. R matrix
		<key, value> : <Id of map task, R matrix>
	2. first Q matrix
		<key, value> : <input key, first Q submatrix>
**/
package nthu.scopelab.tsqr;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.Date;
import java.lang.Math;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FSDataOutputStream;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.MatrixWritable;
import nthu.scopelab.tsqr.math.QRF;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.TSQRunner.fileGather;
import nthu.scopelab.tsqr.TSQRunner.Checker;

import org.netlib.lapack.Dgeqrf;
import org.netlib.lapack.Dorgqr;
import org.netlib.util.intW;
import nthu.scopelab.tsqr.math.QRF;

public class QRFirstJob extends TSQRunner{
  
  public static final String V_FACTOR_OUTPUT = "v.factor.output";
  public static final String COLUMN_SIZE = "column.zise";
  public static final String QF_MAT = "QFM";
  private static final boolean debug = false;
  
  public int run(String[] args) throws Exception {
                       
    String inputfile = getArgument("-input",args);                
    String outputfile = getArgument("-output",args);        
    String reduceSchedule = getArgument("-reduceSchedule",args);
    String colsize = getArgument("-colsize",args);
	int mis = Integer.valueOf(getArgument("-mis",args));
	
    String stages[] = reduceSchedule.split(",");
	String rinput = inputfile;
    String routput = outputfile+"/iter-r-";
	
	for(int i=0;i<stages.length;i++)
	{
	String thenumber = Integer.toString(i+1);
	
	JobConf job = new JobConf(getConf(), QRFirstJob.class);
	job.setJobName("QRFirstJob-Iter-"+thenumber);
	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	
	if(i==0)
		job.setMapperClass(MergeMapper.class);
	else
		job.setMapperClass(IdentityMapper.class);
		
	job.setReducerClass(MergeReducer.class); 
    job.setOutputKeyClass(IntWritable.class);
	FileSystem fs = FileSystem.get(job);

	Path Paths[];
	fileGather fgather = new fileGather(new Path(rinput),"part",fs);
	Paths = fgather.getPaths();
	mis = Checker.checkMis(mis,fgather.getInputSize(),fs);
	job.setNumMapTasks(fgather.recNumMapTasks(mis));
	
    job.setOutputValueClass(MatrixWritable.class);
    job.setNumReduceTasks(Integer.parseInt(stages[i]));
	job.setInt(COLUMN_SIZE,Integer.parseInt(colsize));
	
    fs.get(job).delete(new Path(routput+thenumber), true);	
    FileInputFormat.setInputPaths(job, Paths);
	FileOutputFormat.setOutputPath(job, new Path(routput+thenumber));
	//multiple output of firstQ
	MultipleOutputs.addNamedOutput(job,
                                     QF_MAT,
                                     SequenceFileOutputFormat.class,
                                     IntWritable.class,
                                     MatrixWritable.class);
	
    JobClient.runJob(job);
	rinput = routput+thenumber;
	}
     return 0;
    }
public static class MergeJob extends MapReduceBase
{
		protected OutputCollector<IntWritable,MatrixWritable> output;
		protected int colsize;		
		protected int TaskId;
		protected List<cmDenseMatrix> matrix_buffer = new ArrayList<cmDenseMatrix>();
		protected List<Integer> key_buffer = new ArrayList<Integer>();
		protected List<Integer> matRowSize_buffer = new ArrayList<Integer>();
		protected long time1, time2 ,totalTime=0;
		protected int numRows = 0, numCols = 0;
		protected IntWritable okey = new IntWritable();
		protected MatrixWritable ovalue = new MatrixWritable();
		protected MultipleOutputs qmos;
		protected String Mapred;
		@Override
		public void configure(JobConf job){
		 Mapred = job.get("mapred.task.id").split("_")[3];
         TaskId = Integer.parseInt(job.get("mapred.task.id").split("_")[4]);
		 colsize = job.getInt(COLUMN_SIZE,-1);
		 qmos = new MultipleOutputs(job);
        }
        
   public void collect(IntWritable key, MatrixWritable value) throws IOException
   {
			//store sub Matrix
			numCols=value.matNumColumns();
			numRows+=value.matNumRows();
		    matrix_buffer.add(value.getDense().copy());	
			key_buffer.add(new Integer(key.get()));
			matRowSize_buffer.add(new Integer(value.matNumRows()));
   }
   
     @Override
        public void close() throws IOException {
		time1 = new Date().getTime();
		if(!matrix_buffer.isEmpty())
		{
			//initial a DenseMatrix As numRows x numCols
			cmDenseMatrix As = new cmDenseMatrix(numRows,numCols);
			//put the matrix_buffer data into DenxeMatrix As
			int curRowbegingIndex = 0;
			
			while(!matrix_buffer.isEmpty())
			{
			 cmDenseMatrix dmat = matrix_buffer.remove(0);
			 for(int i=0;i<dmat.numRows();i++)
			  for(int j=0;j<dmat.numColumns();j++)
			  {
			   As.set(i+curRowbegingIndex,j,dmat.get(i,j));
			  }
			 curRowbegingIndex+=dmat.numRows();
			}
			// do QR factorization for As
			QRF qrf = QRF.factorize(As);
			cmDenseMatrix outputQ = qrf.getQ();
			cmDenseMatrix outputR = qrf.getR();
			//output the Q and R
			
			//output Q
			 int curindex = 0;
			 double[] splitmatrix = new double[matRowSize_buffer.get(0)*numCols*2];
			 cmDenseMatrix outputSplitQ = new cmDenseMatrix();
			 int srow_size, mat_size;
			 //split matrix
			 while(!key_buffer.isEmpty())
			 {
			  srow_size =  matRowSize_buffer.remove(0);
			  mat_size = srow_size*numCols;
			  
			  for(int i=0;i<mat_size;i++)
			   splitmatrix[i] = outputQ.getData()[i+curindex];
			  okey.set(key_buffer.remove(0));
			  outputSplitQ.set(splitmatrix,srow_size,numCols);
			  ovalue.set(outputSplitQ);
			  qmos.getCollector(QF_MAT,null).collect(okey,ovalue);
			  curindex+=mat_size;
			 }
			 
			 qmos.close();
			okey.set(TaskId);
			ovalue.set(outputR);
			output.collect(okey,ovalue);
			time2 = new Date().getTime();			
			if(debug)
			{
			 totalTime+=time2-time1;
			 System.out.println("Close Time: "+(time2-time1));
			 System.out.println("Total Compute Time: "+totalTime);
			}
			
		}//matrix_buffer.isEmpty()
    }//close
			
}
public static class MergeMapper 
        extends MergeJob implements Mapper<IntWritable, MatrixWritable, IntWritable, MatrixWritable>{			
						
        public void map(IntWritable key, MatrixWritable value, OutputCollector<IntWritable, MatrixWritable> output, Reporter reporter)
            throws IOException {
			      this.output = output;
				  time1 = new Date().getTime();
			      collect(key,value);
				  time2 = new Date().getTime();
				  if(debug)
				  {
				   totalTime+=time2-time1;
				   System.out.println("sub QR decomposition: "+(time2-time1));
				  }
        }
    }
    
public static class MergeReducer 
        extends MergeJob implements Reducer<IntWritable, MatrixWritable, IntWritable, MatrixWritable>{				
        public void reduce(IntWritable key, Iterator<MatrixWritable> values, OutputCollector<IntWritable, MatrixWritable> output, Reporter reporter)
            throws IOException {
			      this.output = output;
            while (values.hasNext()) 
            {
			     collect(key,values.next());
            }
        }
    }
}