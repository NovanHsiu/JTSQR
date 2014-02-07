/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// modify from mahout-6.0 package org.apache.mahout.math.hadoop.stochasticsvd.BtJob
// 2013 Hsiu-Cheng Yu
package nthu.scopelab.tsqr.ssvd;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
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
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.DenseVector;

import nthu.scopelab.tsqr.matrix.MatrixWritable;
import nthu.scopelab.tsqr.matrix.SparseRowBlockWritable;
import nthu.scopelab.tsqr.matrix.SparseRowBlockAccumulator;
import nthu.scopelab.tsqr.matrix.VectorWritable;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.cmUpperTriangDenseMatrix;
import nthu.scopelab.tsqr.TSQRunner;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.QmultiplyJob;
import nthu.scopelab.tsqr.TSQRunner.fileGather;
import nthu.scopelab.tsqr.TSQRunner.Checker;

/**
 * Bt job. For details, see working notes in MAHOUT-376.
 * <P>
 * 
 * Uses hadoop deprecated API wherever new api has not been updated
 * (MAHOUT-593), hence @SuppressWarning("deprecation").
 * <P>
 * 
 * This job outputs either Bt in its standard output, or upper triangular
 * matrices representing BBt partial sums if that's requested . If the latter
 * mode is enabled, then we accumulate BBt outer product sums in upper
 * triangular accumulator and output it at the end of the job, thus saving space
 * and BBt job.
 * <P>
 * 
 * This job also outputs Q and Bt and optionally BBt. Bt is output to standard
 * job output (part-*) and Q and BBt use named multiple outputs.
 * 
 * <P>
 *---
 * part of Modification:
 * 1. Replaced mahout VectorWritable by MatrixWritable for Map Task Input.
 * 2. Rewrites the SparseRowBlockWritable and SparseRowBlockAccumulator.
 * 3. Extends Class of QmultiplyJob to compute Q.
 * 4. Replaced mahout UpperTriangular by cmUpperTriangDenseMatrix.
 */
 
@SuppressWarnings("unchecked")
public class BtJob extends TSQRunner{
  private static final boolean debug = false;
  public static final String SCHEDULE_NUM = "schedule.number";
  public static final String OUTPUT_BT = "part";
  public static final String OUTPUT_BBT = "BBt";
  public static final String PROP_OUTER_PROD_BLOCK_HEIGHT = "block.height";
  public static final String PROP_OUPTUT_BBT_PRODUCTS = "output.bbt";
  public static final String Q_MAT = QmultiplyJob.Q_MAT;
    
  public static void run(Configuration conf,Path[] inputPath,Path btPath,String qrfPath,int k,int p,
                int outerBlockHeight,int reduceTasks,boolean outputBBtProducts,
				String reduceSchedule,int mis) throws Exception {
    boolean outputQ = true;
	
    	
    String stages[] = reduceSchedule.split(",");
	
	JobConf job = new JobConf(conf, BtJob.class);
	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	job.setInt(SCHEDULE_NUM,stages.length);
	job.setInt(PROP_OUTER_PROD_BLOCK_HEIGHT,outerBlockHeight);
	job.setInt(QJob.PROP_K,k);
	job.setInt(QJob.PROP_P,p);
	job.setBoolean(QmultiplyJob.OUTPUT_Q,outputQ);
	job.setBoolean(PROP_OUPTUT_BBT_PRODUCTS,outputBBtProducts);
	job.set(QmultiplyJob.QRF_DIR,qrfPath);
	FileSystem.get(job).delete(btPath, true);		
    
	FileOutputFormat.setOutputPath(job, btPath);
	job.setJobName("BtJob");
	
	job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(SparseRowBlockWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);
	
	job.setMapperClass(BtMapper.class);		
	job.setCombinerClass(OuterProductCombiner.class);
	job.setReducerClass(OuterProductReducer.class);
	
	fileGather fgather = new fileGather(inputPath,"",FileSystem.get(job));
	mis = Checker.checkMis(mis,fgather.getInputSize(),FileSystem.get(job));
	job.setNumMapTasks(fgather.recNumMapTasks(mis));
	
    job.setNumReduceTasks(reduceTasks);
	
	FileInputFormat.setInputPaths(job,inputPath);
	
	if(outputQ){
	 MultipleOutputs.addNamedOutput(job,
                                     QmultiplyJob.Q_MAT,
                                     SequenceFileOutputFormat.class,
                                     IntWritable.class,
                                     MatrixWritable.class);
	}
	if(outputBBtProducts){
      MultipleOutputs.addNamedOutput(job,
                                     OUTPUT_BBT,
                                     SequenceFileOutputFormat.class,
                                     IntWritable.class,
                                     VectorWritable.class);
    }
	 RunningJob rj = JobClient.runJob(job);
	 System.out.println("Btjob Job ID: "+rj.getJobID().toString());
    }

	public static class BtMapper //only work on iteration 1 (index 0)
        extends QmultiplyJob.readAtoBuildQMethod implements Mapper<IntWritable, MatrixWritable, IntWritable, SparseRowBlockWritable>{	
		private Vector btRow;
		private SparseRowBlockAccumulator btCollector;
		private MultipleOutputs mos;
		private OutputCollector<IntWritable, SparseRowBlockWritable> Btoutput;
		private int kp, height;
		
		private long start, end;
		private long confstart, mapstart, time1, time2, setBtAcc = 0, mapAcc = 0;
		private long computationTime = 0, tag0, tag1;
		private boolean outputQ;
		private Reporter reporter;
		
		@Override
		public void configure(JobConf job){
		
		confstart = new Date().getTime();
		tag0 = confstart;
		super.configure(job);
		System.out.println(job.get("map.input.file"));
		 try{
		 outputQ = job.getBoolean(QmultiplyJob.OUTPUT_Q,false);		 
		 mos = new MultipleOutputs(job);
		 int k = job.getInt(QJob.PROP_K, -1);
         int p = job.getInt(QJob.PROP_P, -1);
		 kp = k+p;
		 //initial
		btRow = new DenseVector(kp);		
		if(k<=0 || p<0)
		 throw new InvalidAlgorithmParameterException("invalid parameter p or k!");	
		 		 
		 height = job.getInt(PROP_OUTER_PROD_BLOCK_HEIGHT,-1);		 
		 btCollector = new SparseRowBlockAccumulator(height,Btoutput);		
		}
		catch(Exception e)
		{
		 e.printStackTrace();
		 throw new NullPointerException("Exception!");
		}
		
		 time1 = new Date().getTime();
		 if(debug)
		 System.out.println("map_conf: "+(time1-confstart));
		 tag1 = time1;
		 computationTime+=tag1-tag0;
		}
		
        public void map(IntWritable key, MatrixWritable value, OutputCollector<IntWritable, SparseRowBlockWritable> output, Reporter reporter)
            throws IOException {
			mapstart = new Date().getTime();
			tag0 = mapstart;
			
			//Build Q
			BuildQ(key,value);
			
			this.reporter = reporter;
			btCollector.setDelegate(output);

			start = new Date().getTime(); //experiment						
			int m = value.matNumRows();						
			end = new Date().getTime(); //experiment
			time1 = new Date().getTime();

			for(int i=0;i<m;i++)
			{
			 if(value.isDense())
			 {
			  //A is dense matrix
			  double[] dARow = value.getDense().getRow(i);
			  for(int j=0;j<dARow.length;j++)
			  {
			   for (int k = 0; k < kp; k++)
			    btRow.set(k, dARow[j] * Q.get(i,k));
			   btCollector.collect((long)j, btRow);
			  }			  
			 }
			 else
			 {
			 //A is sparse matrix
			 FlexCompRowMatrix As = value.getSparse();
			 Vector aRow = As.getRow(i);
			 for (Iterator<VectorEntry> iter = aRow.iterator(); iter.hasNext();)
			 {
			  VectorEntry ev = iter.next();
			  double mul = ev.get();
			  for (int j = 0; j < kp; j++)
				btRow.set(j, mul * Q.get(i,j));
			  btCollector.collect((long)ev.index(), btRow);
			 }
			}
			 //column
			}//for i row
			
			 time2 = new Date().getTime();
			 setBtAcc+=time2-time1;
			 if(outputQ)
			 {
			  value.set(Q);
			  mos.getCollector(QmultiplyJob.Q_MAT, null).collect(key, value);
			 }
			 time2 = new Date().getTime();
			 tag1 = time2;
			 computationTime+=tag1-tag0;		 
        }
		
		@Override
		public void close() throws IOException {
			btCollector.close();
			mos.close();
			time2 = new Date().getTime();
		}
    }
	
	public static class OuterProductCombiner
      extends MapReduceBase implements Reducer<IntWritable, SparseRowBlockWritable, IntWritable, SparseRowBlockWritable> {
    protected final SparseRowBlockWritable accum = new SparseRowBlockWritable();
	
    protected int blockHeight;
	private long confstart, time1, time2, comAcc = 0;
    @Override
    public void configure(JobConf job)
	{
	 confstart = new Date().getTime();
      blockHeight =
        job.getInt(PROP_OUTER_PROD_BLOCK_HEIGHT, -1);
	 int k = job.getInt(QJob.PROP_K, -1);
     int p = job.getInt(QJob.PROP_P, -1);
    }
	
    public void reduce(IntWritable key,
                          Iterator<SparseRowBlockWritable> values,
                          OutputCollector<IntWritable, SparseRowBlockWritable> output, 
						  Reporter reporter) throws IOException
    {
	 time1 = new Date().getTime();
	  
      while(values.hasNext()) {
		SparseRowBlockWritable bw = values.next();
        accum.plusBlock(bw);
      }
      output.collect(key, accum); //ovalue);
      accum.clear();
	  time2 = new Date().getTime();
	 comAcc+=time2-time1;
    }
	 @Override
    public void close() throws IOException
    {
	 time2 = new Date().getTime();
	 if(debug)
	 {
	 System.out.println("comAcc: "+comAcc);
	 System.out.println("total Time: "+(time2-confstart));
	 }
	}
  }
  
  public static class OuterProductReducer
      extends MapReduceBase implements Reducer<IntWritable, SparseRowBlockWritable, IntWritable, VectorWritable> {

    protected final SparseRowBlockWritable accum = new SparseRowBlockWritable();
	
    protected int blockHeight;
    private boolean outputBBt;
    private cmUpperTriangDenseMatrix mBBt;
    private MultipleOutputs mos;
    private IntWritable btKey = new IntWritable();
    private VectorWritable btValue = new VectorWritable();
	private Vector btRow;
	private long confstart, redstart, time1, time2, mBBtAcc = 0, redAcc = 0;
	private int kp;
    @Override
    public void configure(JobConf job)
    {
		confstart = new Date().getTime();
      blockHeight =
        job.getInt(PROP_OUTER_PROD_BLOCK_HEIGHT, -1);

      outputBBt =
        job.getBoolean(PROP_OUPTUT_BBT_PRODUCTS, false);
	  
	  try{
      if (outputBBt) {
        int k = job.getInt(QJob.PROP_K, -1);
        int p = job.getInt(QJob.PROP_P, -1);
		
		if(k<=0 || p<0)
		 throw new InvalidAlgorithmParameterException("invalid parameter p or k!");	
		kp = k+p;
        mBBt = new cmUpperTriangDenseMatrix(kp);
        mos = new MultipleOutputs(job);
      }
	  }
	  catch(Exception exc)
	  {
		exc.printStackTrace();
	  }
	  time2 = new Date().getTime();
	  if(debug)
	   System.out.println("conf time: "+(time2-confstart));
    }

    public void reduce(IntWritable key,
                          Iterator<SparseRowBlockWritable> values,
                          OutputCollector<IntWritable, VectorWritable> output, 
						  Reporter reporter) throws IOException
    {
	  redstart = new Date().getTime();
      //accum.clear();
	  
      while (values.hasNext()) {
		SparseRowBlockWritable bw = values.next();
       accum.plusBlock(bw);
      }

      /*
       * at this point, sum of rows should be in accum, so we just generate
       * outer self product of it and add to BBt accumulator.
       */
	  time1 = new Date().getTime();
      for (int k = 0; k < accum.getNumRows(); k++) { 
        Vector btRow = accum.getRows()[k];
        btKey.set((int) (key.get() * blockHeight + accum.getRowIndices()[k]));//k));
        btValue.set((DenseVector)btRow);
        output.collect(btKey, btValue);

        if (outputBBt) {
          int kp = mBBt.numRows();
          // accumulate partial BBt sum
          for (int i = 0; i < kp; i++) {
            double vi = btRow.get(i);
            if (vi != 0.0) {
              for (int j = i; j < kp; j++) {
                double vj = btRow.get(j);
                if (vj != 0.0) {
                  mBBt.set(i, j, mBBt.get(i, j) + vi * vj);
                }
              }
            }
          }
        }
      }
	  time2 = new Date().getTime();
	  mBBtAcc+=time2-time1;
	  redAcc+=time2-redstart;
    }
	
    @Override
    public void close() throws IOException
    {
      // if we output BBt instead of Bt then we need to do it.
	  if (outputBBt) {
      try {
        mos.getCollector(OUTPUT_BBT, null).collect(new IntWritable(),new VectorWritable(new DenseVector(mBBt.getData())));
      } finally {
        mos.close();
      }
	  }
	  time2 = new Date().getTime();
	  if(debug)
	  {
	  System.out.println("mBBtAcc: "+mBBtAcc);
	  System.out.println("redAcc: "+redAcc);
	  System.out.println("total Time: "+(time2-confstart));
	  }
    }
  }
  private static class Melement
  {
         int m, n;
         double e;
         public Melement(int m,int n,double e)
         {
          this.m = m; this.n = n; this.e = e;
         }
         public void set(int m,int n,double e)
         {
          this.m = m; this.n = n; this.e = e;
         }
         public double get()
         {
          return e;
         }
         public int row()
         {
          return m;
         }
         public int column()
         {
          return n;
         }
   }

}