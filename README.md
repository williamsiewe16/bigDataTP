# TP YARN

You are going to write some MapReduce jobs on the remarkable trees of Paris
using this dataset.

## 1- Districts containing trees

``DistrictMapper.java ``

```java
public class DistrictMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");
            word.set(tokens[1]);
            context.write(word, NullWritable.get());
        }
    }
}
```
``DistrictReducer.java ``
```java
public class DistrictReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
```

## 2- Show all existing species

``ExistingSpeciesMapper.java ``
```java
public class ExistingSpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");
            word.set(tokens[3]);
            context.write(word, NullWritable.get());
        }
    }
}
```

``ExistingSpeciesReducer.java ``
```java
public class ExistingSpeciesReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
```

## 3- Number of trees by kinds

``CountTreesByKindsMapper.java ``

```java
public class CountTreesByKindsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");
            word.set(tokens[2]);
            context.write(word, new IntWritable(1));
        }
    }
}
```

``CountTreesByKindsReducer.java ``

```java
public class CountTreesByKindsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for(IntWritable value: values){
            sum+=1;
        }
        context.write(key, new IntWritable(sum));
    }
}
```

## 4- Maximum height per kind of tree

``MaxHeightPerKindMapper.java ``

```java
public class MaxHeightPerKindMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");
            word.set(tokens[2]);
            context.write(word, new FloatWritable(Float.parseFloat(tokens[6])));
        }
    }
}
```

``MaxHeightPerKindReducer.java ``

```java
public class MaxHeightPerKindReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float max=0;
        for(FloatWritable value: values){
            if(max < value.get()) max = value.get();
        }
        context.write(key, new FloatWritable(max));
    }
}
```

## 5- Sort the trees height from smallest to largest

``SortByHeightMapper.java ``

```java
public class SortByHeightMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");
            context.write(new IntWritable(1), new FloatWritable(Float.parseFloat(tokens[7])));
        }
    }
}
```


``SortByHeightReducer.java ``

```java
public class SortByHeightReducer extends Reducer<IntWritable, FloatWritable, FloatWritable, NullWritable> {

    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<FloatWritable> list = Lists.newArrayList(values);
        Collections.sort(list);
        for(FloatWritable i : list){
            context.write(i, NullWritable.get());
        }
    }
}
```


## 6- District containing the oldest tree

``OldestTreeDistrictMapper.java ``

```java
public class OldestTreeDistrictMapper extends Mapper<Object, Text, IntWritable, MapWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");

            if(!tokens[1].isEmpty() & !tokens[5].isEmpty()){
                int arrondissement = Integer.parseInt(tokens[1]);
                int annee = Integer.parseInt(tokens[5]);
                MapWritable result = new MapWritable();
                result.put(new Text("arrondissement"),new IntWritable(arrondissement));
                result.put(new Text("annee"),new IntWritable(annee));

                context.write(new IntWritable(1), result);
            }
        }
    }
}
```

``OldestTreeDistrictReducer.java ``

```java
public class OldestTreeDistrictReducer extends Reducer<IntWritable, MapWritable, IntWritable, NullWritable> {

    public void reduce(IntWritable key, Iterable<MapWritable> tuples, Context context) throws IOException, InterruptedException {
        int minYear=9999; int arrondissement_=0;
        for(MapWritable tuple: tuples){
            int arrondissement = ((IntWritable)tuple.get(new Text("arrondissement"))).get();
            int annee = ((IntWritable)tuple.get(new Text("annee"))).get();
            System.out.println("no");
            if(minYear >= annee){
                System.out.println("good");
                minYear = annee;
                arrondissement_ = arrondissement;
            }
        }
        context.write(new IntWritable(arrondissement_), NullWritable.get());
    }
}
```


## 7- District containing the most trees

``MostTreesDistrictMapper1.java ``

```java
public class MostTreesDistrictMapper1 extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");
            int district = Integer.parseInt(tokens[1]);
            context.write(new IntWritable(district), new IntWritable(1));
        }
    }
}
```

``MostTreesDistrictReducer1.java ``

```java
public class MostTreesDistrictReducer1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for(IntWritable value: values){
            sum+=value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
```

``MostTreesDistrictMapper2.java ``

```java
public class MostTreesDistrictMapper2 extends Mapper<Object, Text, IntWritable, IntTupleWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String [] values = value.toString().split("\t");
        System.out.println(values[1]);
        int district = Integer.parseInt(values[0]);
        int count = Integer.parseInt(values[1]);
        context.write(new IntWritable(1), new IntTupleWritable(district,count));
    }
}
```

``MostTreesDistrictReducer2.java ``

```java
public class MostTreesDistrictReducer2 extends Reducer<IntWritable, IntTupleWritable, IntWritable, NullWritable> {

    public void reduce(IntWritable key, Iterable<IntTupleWritable> tuples, Context context) throws IOException, InterruptedException {
        int max=0; int arrondissement=0;
        for(IntTupleWritable tuple: tuples){
            int [] tupleValues = tuple.get();
            if(max <= tupleValues[1]){
                max = tupleValues[1];
                arrondissement = tupleValues[0];
            }
        }
        context.write(new IntWritable(arrondissement), NullWritable.get());
    }
}
```