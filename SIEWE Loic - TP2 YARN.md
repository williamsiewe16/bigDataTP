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

``DistrictMapperTest.java ``
```java
    public void testMap() throws IOException, InterruptedException {
        String value = "foo;1;tux";
        this.districtMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("1"), NullWritable.get());
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

``DistrictReducerTest.java ``
```java
    @Test
public void testReduce() throws IOException, InterruptedException {
        String key = "1";
        NullWritable value = NullWritable.get();
        Iterable<NullWritable> values = Arrays.asList(value, value, value);
        this.districtReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), value);
        }
```

```bash
[loic.william.siewe.dahe@hadoop-edge01 ~]$ hdfs dfs -cat tp2/listDistrict/part-r-00000
11
12
13
14
15
16
17
18
19
20
3
4
5
6
7
8
9
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

``ExistingSpeciesMapperTest.java ``
```java
    @Test
public void testMap() throws IOException, InterruptedException {
        String value = "foo;1;tux;sephora";
        this.existingSpeciesMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
        .write(new Text("sephora"), NullWritable.get());
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

``ExistingSpeciesReducerTest.java ``
```java
    @Test
public void testReduce() throws IOException, InterruptedException {
        String key = "sephora";
        NullWritable value = NullWritable.get();
        Iterable<NullWritable> values = Arrays.asList(value, value, value);
        this.existingSpeciesReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), value);
        }
```

```java
[loic.william.siewe.dahe@hadoop-edge01 ~]$ hdfs dfs -cat tp2/listExistingSpecies/part-r-00000
araucana
atlantica
australis
baccata
bignonioides
biloba
bungeana
cappadocicum
carpinifolia
colurna
coulteri
decurrens
dioicus
distichum
excelsior
fraxinifolia
giganteum
giraldii
glutinosa
grandiflora
hippocastanum
ilex
involucrata
japonicum
kaki
libanii
monspessulanum
nigra
nigra laricio
opalus
orientalis
papyrifera
petraea
pomifera
pseudoacacia
sempervirens
serrata
stenoptera
suber
sylvatica
tomentosa
tulipifera
ulmoides
virginiana
x acerifolia
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

``CountTreesByKindsMapperTest.java ``
```java
    @Test
public void testMap() throws IOException, InterruptedException {
        String value = "foo;1;tux;sephora";
        this.countTreesByKindsMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
        .write(new Text("tux"), new IntWritable(1));
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

``CountTreesByKindsReducerTest.java ``
```java
    @Test
public void testReduce() throws IOException, InterruptedException {
        String key = "sephora";
        IntWritable value = new IntWritable(1);
        Iterable<IntWritable> values = Arrays.asList(value, value, value);
        this.countTreesByKindsReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), new IntWritable(3));
        }
```

```bash
[loic.william.siewe.dahe@hadoop-edge01 ~]$ hdfs dfs -cat tp2/countTreesByKinds/part-r-00000
Acer	3
Aesculus	3
Ailanthus	1
Alnus	1
Araucaria	1
Broussonetia	1
Calocedrus	1
Catalpa	1
Cedrus	4
Celtis	1
Corylus	3
Davidia	1
Diospyros	4
Eucommia	1
Fagus	8
Fraxinus	1
Ginkgo	5
Gymnocladus	1
Juglans	1
Liriodendron	2
Maclura	1
Magnolia	1
Paulownia	1
Pinus	5
Platanus	19
Pterocarya	3
Quercus	4
Robinia	1
Sequoia	1
Sequoiadendron	5
Styphnolobium	1
Taxodium	3
Taxus	2
Tilia	1
Ulmus	1
Zelkova	4
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

``MaxHeightPerKindMapperTest.java ``
```java
    @Test
public void testMap() throws IOException, InterruptedException {
        String value = "foo;1;tux;sephora;tom;pa;12.0;4";
        this.maxHeightPerKindMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
        .write(new Text("tux"), new FloatWritable(12.0F));
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

``MaxHeightPerKindReducerTest.java ``
```java
    @Test
public void testReduce() throws IOException, InterruptedException {
        String key = "sephora";
        FloatWritable max = new FloatWritable(15);
        FloatWritable value1 = new FloatWritable(12);
        FloatWritable value2 = new FloatWritable(10);
        Iterable<FloatWritable> values = Arrays.asList(value1, max, value2);
        this.maxHeightPerKindReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), max);
        }
```


```bash
[loic.william.siewe.dahe@hadoop-edge01 ~]$ hdfs dfs -cat tp2/maxHeightPerKind/part-r-00000
Acer	16.0
Aesculus	30.0
Ailanthus	35.0
Alnus	16.0
Araucaria	9.0
Broussonetia	12.0
Calocedrus	20.0
Catalpa	15.0
Cedrus	30.0
Celtis	16.0
Corylus	20.0
Davidia	12.0
Diospyros	14.0
Eucommia	12.0
Fagus	30.0
Fraxinus	30.0
Ginkgo	33.0
Gymnocladus	10.0
Juglans	28.0
Liriodendron	35.0
Maclura	13.0
Magnolia	12.0
Paulownia	20.0
Pinus	30.0
Platanus	45.0
Pterocarya	30.0
Quercus	31.0
Robinia	11.0
Sequoia	30.0
Sequoiadendron	35.0
Styphnolobium	10.0
Taxodium	35.0
Taxus	13.0
Tilia	20.0
Ulmus	15.0
Zelkova	30.0
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


``SortByHeightMapperTest.java ``
```java
    @Test
public void testMap() throws IOException, InterruptedException {
        String value = "foo;1;tux;sephora;tom;pa;12.0;4";
        this.sortByHeightMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
        .write(new IntWritable(1), new FloatWritable(12.0F));
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

``SortByHeightReducerTest.java ``
```java
    @Test
public void testReduce() throws IOException, InterruptedException {
        FloatWritable i = new FloatWritable(11.0F);
        FloatWritable i1 = new FloatWritable(9.0F);
        FloatWritable i2 = new FloatWritable(8.0F);
        Iterable<FloatWritable> values = Arrays.asList(i,i1,i2);
        this.sortByHeightReducer.reduce(new IntWritable(1), values, this.context);
        verify(this.context).write(i2, NullWritable.get());
        verify(this.context).write(i1, NullWritable.get());
        verify(this.context).write(i, NullWritable.get());
        }
```

```bash

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

``OldestTreeDistrictMapperTest.java ``
```java
    @Test
public void testMap() throws IOException, InterruptedException {
        MapWritable map = new MapWritable();
        map.put(new Text("arrondissement"),new IntWritable(6));
        map.put(new Text("annee"),new IntWritable(1605));
        String value = "foo;6;17;sephora;tom;1605;12.0;4";
        this.oldestTreeDistrictMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
        .write(new IntWritable(1), map);
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

``OldestTreeDistrictReducerTest.java ``
```java
    @Test
public void testReduce() throws IOException, InterruptedException {
        MapWritable map1 = new MapWritable();
        map1.put(new Text("arrondissement"),new IntWritable(6));
        map1.put(new Text("annee"),new IntWritable(1605));
        MapWritable map2 = new MapWritable();
        map2.put(new Text("arrondissement"),new IntWritable(5));
        map2.put(new Text("annee"),new IntWritable(1492));
        MapWritable map3 = new MapWritable();
        map3.put(new Text("arrondissement"),new IntWritable(4));
        map3.put(new Text("annee"),new IntWritable(1952));
        Iterable<MapWritable> values = Arrays.asList(map1,map3,map2);
        this.oldestTreeDistrictReducer.reduce(new IntWritable(1), values, this.context);
        verify(this.context).write(new IntWritable(5), NullWritable.get());
        }
```

```bash
[loic.william.siewe.dahe@hadoop-edge01 ~]$ hdfs dfs -cat tp2/oldestTreesDistrict/part-r-00000
5
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

``MostTreesDistrictMapperTest1.java ``
```java
    @Test
public void testMap() throws IOException, InterruptedException {
        String value = "foo;1;17;sephora;tom;1605;12.0;4";
        this.mostTreesDistrict.map(null, new Text(value), this.context);
        verify(this.context, times(1))
        .write(new IntWritable(1), new IntWritable(1));
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

``MostTreesDistrictReducerTest1.java ``
```java
    @Test
public void testReduce() throws IOException, InterruptedException {
        IntWritable value = new IntWritable(1);
        Iterable<IntWritable> values = Arrays.asList(value, value, value);
        this.mostTreesDistrictReducer1.reduce(value, values, this.context);
        verify(this.context).write(value, new IntWritable(3));
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

``MostTreesDistrictMapperTest2.java ``
```java
    @Test
public void testMap() throws IOException, InterruptedException {
        String value = "1\t4";
        this.mostTreesDistrictMapper2.map(null, new Text(value), this.context);
        verify(this.context, times(1))
        .write(new IntWritable(1), new IntTupleWritable(1,4));
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

``MostTreesDistrictReducerTest2.java ``
```java
    @Test
public void testReduce() throws IOException, InterruptedException {
        IntTupleWritable i = new IntTupleWritable(17,5);
        IntTupleWritable max = new IntTupleWritable(8,12);
        IntTupleWritable i1 = new IntTupleWritable(5,8);
        Iterable<IntTupleWritable> values = Arrays.asList(i,i1,max);
        this.mostTreesDistrictReducer2.reduce(new IntWritable(1), values, this.context);
        verify(this.context).write(new IntWritable(max.get()[0]), NullWritable.get());
        }
```

```bash
[loic.william.siewe.dahe@hadoop-edge01 ~]$ hdfs dfs -cat tp2/mostTreesDistrict/out/part-r-00000
16
```