//����Mapper
public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, MyWritable> {  
    private Logger logger = LoggerFactory.getLogger(KMeansMapper.class);  
    private String centerPathStr="";  
    private String splitter ="";  
    private int k;// �洢�������ĸ���  
    private String[] centerVec= null; // �洢������������  
      
    @Override  
    protected void setup(Context context)  
            throws IOException, InterruptedException {  
        centerPathStr = context.getConfiguration().get(Utils.CENTERPATH);  
        splitter = context.getConfiguration().get(Utils.SPLITTER);  
        k = context.getConfiguration().getInt(Utils.K, 0);  
        centerVec  = new String[k];  
          
        // TODO ��ȡ�������ĵ����� centerVec��  
        Path path = new Path(centerPathStr);  
        FSDataInputStream is=Utils.getFs().open(path);  
        BufferedReader br=new BufferedReader(new InputStreamReader(is));  
        String line="";  
        int i=0;  
        while( (line=br.readLine())!=null){  
         centerVec[i++]=line;  
    }  
        br.close();  
        is.close();  
  
    }  
      
    private IntWritable ID = new IntWritable();  
    private MyWritable mw = new MyWritable();  
    @Override  
    protected void map(LongWritable key, Text value, Context context)  
            throws IOException, InterruptedException {  
        int vecId = getCenterId(value.toString());  
          
        ID.set(vecId);  
//      logger.info("**********************************"+vecId);  
        mw.setData(value.toString());  
        context.write(ID, mw);  
//      logger.info("ID:{},value:{}",new Object[]{vecId,value});  
    }  
      
    /** 
     * ���㵱ǰ�е��������������о�����С���±ꣻ 

     */  
    private int getCenterId(String line) {  
        int type=-1;  
        double min=Double.MAX_VALUE;  
        double distance=0.0;  
        for(int i=0;i<centerVec.length;i++){  
            distance=Utils.calDistance(line,centerVec[i],splitter);  
            if(distance<min){  
                min=distance;  
                type=i;  
            }  
        }  
        return type;  
        }  
  
}  

//����ŷʽ����
public static double calDistance(String line, String string,String splitter) {  
    double sum=0;  
    String[] data=line.split(splitter);  
    String[] centerI=string.split(splitter);  
    for(int i=0;i<data.length;i++){  
        sum+=Math.pow(Double.parseDouble(data[i])-Double.parseDouble(centerI[i]), 2);  
    }  
    return Math.sqrt(sum);  
}  



public class MyWritable implements Writable {  
      
    private int num = 1;  
    private String data;  
    public MyWritable() {  
        // TODO Auto-generated constructor stub  
    }  
    public MyWritable(int num, String data){  
        this.num = num;  
        this.data = data;  
    }  
    @Override  
    public void write(DataOutput out) throws IOException {  
        // TODO Auto-generated method stub  
        out.writeInt(num);  
        out.writeUTF(data);  
    }  
  
  
    @Override  
    public void readFields(DataInput in) throws IOException {  
        // TODO Auto-generated method stub  
        num = in.readInt();  
        data = in.readUTF();  
    }  
    public int getNum() {  
        return num;  
    }  
  
    public void setNum(int num) {  
        this.num = num;  
    }  
  
    public String getData() {  
        return data;  
    }  
  
    public void setData(String data) {  
        this.data = data;  
    }  
  
}  

public class KmeansCombiner extends Reducer<IntWritable, MyWritable, IntWritable, MyWritable>{  
    private String splitter ;  
    private Pattern pattern;  
    @Override  
    protected void setup(Context context)  
            throws IOException, InterruptedException {  
        splitter = context.getConfiguration().get(SPLITTER);  
        pattern = Pattern.compile(",");  
    }  

    MyWritable result = new MyWritable();  
    @Override  
    protected void reduce(IntWritable key, Iterable<MyWritable> values,  
            Context context)  
            throws IOException, InterruptedException {  
        double[] sum=null;  
        long  num =0;  
        for(MyWritable value:values){  
            String[] valStr = pattern.split(value.getData().toString(), -1);  
            if(sum==null){// ��ʼ��  
                sum=new double[valStr.length];  
                addToSum(sum,valStr);// ��һ����Ҫ����  
            }else{  
            //  ��Ӧ�ֶ����  
                addToSum(sum,valStr);  
            }  
            num++;            
        }  
        result.setData(format(sum));  
        result.setNum((int) num);  
        context.write(key, result);  
    }  
      
    /** 
     * ��Ӧ�ֶ���� 
     * @param sum 
     * @param valStr 
     */  
        private void addToSum(double[] sum, String[] valStr) {  
            //  ʵ�ֹ���  
            for(int i=0;i<sum.length;i++){  
                sum[i]+=Double.parseDouble(valStr[i]);  
            }  
  
        }  
        private String format(double[] sum) {  
            //���ƹ���  
            String str="";  
            for(int i=0;i<sum.length;i++){  
                if(i==0){  
                    str=str.concat(String.valueOf(sum[i]));  
                }else{  
                    str=str.concat(splitter+String.valueOf(sum[i]));  
                }  
            }  
            return str;  
        }  
}  

public class KMeansReducer extends Reducer<IntWritable, MyWritable, Text, NullWritable> {  
  
    private String splitter ;  
    private Pattern pattern;  
    private String[] centerVec = null;  
    private int k;  
      
    private Logger log = LoggerFactory.getLogger(KMeansReducer.class);  
    @Override  
    protected void setup(Context context)  
            throws IOException, InterruptedException {  
        splitter = context.getConfiguration().get(SPLITTER);  
        pattern = Pattern.compile(",");  
        k= context.getConfiguration().getInt(K, 0);  
        centerVec = new String[k];  
    }  
      
      
    @Override  
    protected void reduce(IntWritable key, Iterable<MyWritable> values,  
            Context arg2) throws IOException, InterruptedException {  
        double[] sum=null;  
        long  num =0;  
        for(MyWritable value:values){  
            int number = value.getNum();  
            String[] valStr = pattern.split(value.getData().toString(), -1);  
            if(sum==null){// ��ʼ��  
                sum=new double[valStr.length];  
                addToSum(sum,valStr);// ��һ����Ҫ����  
            }else{  
            //  ��Ӧ�ֶ����  
                addToSum(sum,valStr);  
            }  
            num += number;            
        }  
        averageSum(sum,num);  
        centerVec[key.get()]= format(sum);  
    }  
    private Text vec = new Text();  
    /** 
     * ֱ���������centerVec 
     */  
    @Override  
    protected void cleanup(Context context)  
            throws IOException, InterruptedException {  
        for(int i=0;i<centerVec.length;i++ ){  
              
            vec.set(centerVec[i]);  
            context.write(vec, NullWritable.get());  
        }  
    }  
  
/** 
 * ��ƽ��ֵ 
 * @param sum 
 * @param num 
 */  
    private void averageSum(double[] sum, long num) {  
        //��ƽ��ֵ  
          
        for(int i=0;i<sum.length;i++){  
            sum[i]=sum[i]/num;  
        }  
      
    }  
  
/** 
 * ��Ӧ�ֶ���� 
 * @param sum 
 * @param valStr 
 */  
    private void addToSum(double[] sum, String[] valStr) {  
        //  ʵ�ֹ���  
        for(int i=0;i<sum.length;i++){  
            sum[i]+=Double.parseDouble(valStr[i]);  
        }  
  
    }  
  
/** 
 * ��ʽ������ 
 * ����Ԫ��֮��ķָ�������splitter���� 
 * @param sum 
 * @return 
 */  
    private String format(double[] sum) {  
        //���ƹ���  
        String str="";  
        for(int i=0;i<sum.length;i++){  
            if(i==0){  
                str=str.concat(String.valueOf(sum[i]));  
            }else{  
                str=str.concat(splitter+String.valueOf(sum[i]));  
            }  
        }  
        return str;  
    }  
}  


public class Alljobs {  
  
    public static void main(String[] args) throws Exception {  
        // TODO Auto-generated method stub  
        String[] KmeansArgs = new String[]{  
                    "Kmeans_Data.txt", //ԭʼ����  
                    "Kmeans_result.txt", // ����������ݵ����  
                    "4", // �ۼ���  
                    ",", // ԭʼ���ݷָ���  
                    "60",// ��������  
                    "0.5", // �����ֵ  
                    "0" //start: 0-> ��ʼ���������ģ�1 -> �����µľ�������  2 -> �Ƿ����  
        };  
        String input = KmeansArgs[0];  
        String output = KmeansArgs[1];  
        int k = Integer.valueOf(KmeansArgs[2]);  
        String splitter = KmeansArgs[3];  
        int iteration = Integer.valueOf(KmeansArgs[4]);  
        double delta = Double.parseDouble(KmeansArgs[5]);  
        int start = Integer.valueOf(KmeansArgs[6]);  
        int number = 0;  
        // 1. ��ʼ��������������(SampleJob)  
        int ret = -1;  
        String fileStr = "iter";  
        switch (start){  
        case 0 :first(input, output, k, ret);  
        case 1: number = updateKmeans(input, output, k,   
                splitter, delta, ret, iteration);  
        case 2: if(start == 2){  
            number = readLastFile(output,fileStr)-1;  
            }  
            classify(number, input, output, k,   
                splitter, iteration);  
        default: break;  
        }  
    }  
    public static void first(String input,String output,  
            int k,  
            int ret) throws Exception{  
        String[] job1Args = new String[]{  
                input,  
                output+"/iter0",  
                String.valueOf(k)  
        };  
        ret = ToolRunner.run(Utils.getConf(),new Driver.MyDriver(), job1Args);  
        if(ret != 0){  
            System.err.println("sample job failed!");  
            System.exit(-1);  
        }  
    }  
    // 2. ѭ��Kmeans�����¾�������  
            public static int updateKmeans(String input,String output,  
                    int k,String splitter,double delta,  
                    int ret,int iteration) throws Exception{   
                int num = 0;  
                for(int i=0;i<iteration;i++){  
                    String[] jobArgs = new String[]{  
                            input, // input  
                            output+"/iter"+(i+1),  //��ǰ��������  
                            splitter, // splitter  
                            String.valueOf(k),  
                            output+"/iter"+i+"/part-r-00000" // ��һ�ξ�������  
                    };  
                    ret = ToolRunner.run(Utils.getConf(), new KMeansDriver(), jobArgs);  
                    if(ret != 0){  
                        System.err.println("kmeans job failed!"+":"+i);  
                        System.exit(-1);  
                    }  
                    if(!Utils.shouldRunNextIteration(output+"/iter"+i+"/part-r-00000",output+"/iter"+(i+1)+"/part-r-00000",  
                            delta,splitter)){  
                        num = i+1;  
                        break;  
                    }  
                }  
                return num;  
            }  
            // 3. ����  
    public static void classify(int num,String input,String output,  
            int k,String splitter,  
            int iteration) throws IOException, ClassNotFoundException, InterruptedException{  
        if (num == 0) {  
            num = iteration;  
        }  
        Configuration conf = Utils.getConf();  
        conf.set(SPLITTER, splitter );  
        conf.set(CENTERPATH, output+"/iter"+num+"/part-r-00000");  
        conf.setInt(K, k);  
        Job job = Job.getInstance(conf,"classify");  
        job.setMapperClass(Classify.KMeansMapper.class);  
        job.setPartitionerClass(Classify.KmeansPartional.class);  
        job.setReducerClass(Classify.ClassifyReducer.class);  
        job.setMapOutputKeyClass(IntWritable.class);  
        job.setMapOutputValueClass(Text.class);  
        job.setOutputKeyClass(IntWritable.class);  
        job.setOutputValueClass(Text.class);  
        job.setNumReduceTasks(k);  
        FileInputFormat.addInputPath(job, new Path(input));  
        Path out =new Path(output+"/clustered");  
        FileOutputFormat.setOutputPath(job,out);  
        if(Utils.getFs().exists(out)){  
            Utils.getFs().delete(out, true);  
        }  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }  
    public static int readLastFile(String output,String fileStr) throws IOException{  

        Path path = new Path(output);  
        FileStatus[] fs = Utils.getFs().listStatus(path);  
        int num = 0;  
        for(int i=0;i<fs.length;i++){  
            if(fs[i].getPath().getName().startsWith(fileStr)){  
                num++;  
            }  
        }  
        return num;   
    }  
}  