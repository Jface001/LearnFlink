package com.test.flink.transformation;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/5 20:26
 * @Desc: FLink 批处理的 DataSet  转换函数的基本使用
 */
public class BatchBasicDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.准备数据-source
        DataSource<String> inputDataSet = env.readTextFile("datas/click.log");
        //3.处理数据-transformation
        //TODO:1、【map函数】，将JSON转换为JavaBean对象
        MapOperator<String, ClickLog> clickDataSet = inputDataSet.map(new MapFunction<String, ClickLog>() {
            @Override
            public ClickLog map(String value) throws Exception {
                return JSON.parseObject(value, ClickLog.class);
            }
        });
        clickDataSet.first(10).printToErr();
        System.out.println("=================");
        //TODO:2、【flatMap】，每条数据转换为日期时间格式
/*        Long类型日期时间：	1577890860000
                |
							|进行格式
                |
                String类型日期格式
        yyyy-MM-dd-HH
        yyyy-MM-dd
        yyyy-MM   */

        FlatMapOperator<ClickLog, String> flatmapDataSet = clickDataSet.flatMap(new FlatMapFunction<ClickLog, String>() {
            @Override
            public void flatMap(ClickLog value, Collector<String> out) throws Exception {
                //获取访问数据
                Long entryTime = value.getEntryTime();
                //格式1；yyyy-MM-dd-HH
                String hour = DateFormatUtils.format(entryTime, "yyyy-MM-dd-HH");
                out.collect(hour);
                //格式2：yyyy-MM-dd-
                String day = DateFormatUtils.format(entryTime, "yyyy-MM-dd");
                out.collect(day);
                //格式3：yyyy-MM
                String month = DateFormatUtils.format(entryTime, "yyyy-MM");
                out.collect(month);
            }
        });
        flatmapDataSet.first(10).printToErr();
        System.out.println("=================");
        //TODO:3、【filter函数】，过滤使用谷歌浏览器数据
        FilterOperator<ClickLog> filterDataSet = clickDataSet.filter(new FilterFunction<ClickLog>() {
            @Override
            public boolean filter(ClickLog value) throws Exception {
                return "谷歌浏览器".equals(value.getBrowserType());
            }
        });
        filterDataSet.first(10).printToErr();
        System.out.println("=================");
        //TODO:4、【groupBy】函数与【sum】函数，按照浏览器类型分组，统计次数，只能接收元组类型
        //将数据转换成元组形式
        MapOperator<ClickLog, Tuple2<String, Integer>> tupleDataSet = clickDataSet.map(new MapFunction<ClickLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(ClickLog value) throws Exception {
                return Tuple2.of(value.getBrowserType(), 1);
            }
        });
        //分组+求和
        AggregateOperator<Tuple2<String, Integer>> sumDataSet = tupleDataSet.groupBy(0).sum(1);
        sumDataSet.print();
        System.out.println("=================");
        //TODO:5、【min和minBy】函数使用，求取最小值
        AggregateOperator<Tuple2<String, Integer>> minDataSet = sumDataSet.min(1);
        minDataSet.print();//只管求最小值的列，其它列随机
        ReduceOperator<Tuple2<String, Integer>> minByDataSet = minDataSet.minBy(1);
        minByDataSet.print();//筛选出最小值对应的行的数据
        System.out.println("=================");
        //TODO:6、【aggregate】函数，对数据进行聚合操作，需要指定聚合函数和字段，只管理计算的列，其它列随机
        AggregateOperator<Tuple2<String, Integer>> aggregateDataSet = sumDataSet.aggregate(Aggregations.SUM, 1);
        aggregateDataSet.printToErr();
        AggregateOperator<Tuple2<String, Integer>> maxaDataSet = sumDataSet.aggregate(Aggregations.MAX, 1);
        maxaDataSet.printToErr();
        System.out.println("=================");
        //TODO:7、【reduce和reduceGroup】使用，按照浏览器类型分组，统计次数,只能使用元组形式
        //将数据转换成元组，并按照分组
        UnsortedGrouping<Tuple2<String, Integer>> groupDataSet = clickDataSet
                .map(new MapFunction<ClickLog, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(ClickLog value) throws Exception {
                        return new Tuple2(value.getBrowserType(), 1);
                    }
                })
                .groupBy(0);//根据索引下标分组
        //reduce 函数聚合
        ReduceOperator<Tuple2<String, Integer>> reduceDataSet = groupDataSet.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> temp, Tuple2<String, Integer> item) throws Exception {
                String key = temp.f0;// 获取value，就是浏览器名称
                int sum = temp.f1 + item.f1;//累加次数，temp里面是上一次的和
                return Tuple2.of(key, sum);
            }
        });
        reduceDataSet.printToErr();

        //reduceGroup 函数聚合, values 就是分组后 key 对应的所有数据
        GroupReduceOperator<Tuple2<String, Integer>, String> reduceGroupDataSet = groupDataSet.reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, String>() {
            @Override
            public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<String> out) throws Exception {
                //定义变量，获取分组 key
                String key = "";
                int sum = 0;
                //变量 key 对应组内元素
                for (Tuple2<String, Integer> item : values) {
                    key = item.f0;
                    sum += item.f1;
                }
                //输出结果
                out.collect("(" + key + ", " + sum + ")");
            }
        });
        reduceGroupDataSet.print();

        System.out.println("=================");
        //TODO:8、【union函数】，合并数据类型相同2个数据集
        MapOperator<String, ClickLog> dataSet01 = env.readTextFile("datas/input/click1.log")
                .map(new MapFunction<String, ClickLog>() {
                    @Override
                    public ClickLog map(String value) throws Exception {
                        return JSON.parseObject(value, ClickLog.class);
                    }
                });
        MapOperator<String, ClickLog> dataSet02 = env.readTextFile("datas/input/click2.log")
                .map(new MapFunction<String, ClickLog>() {
                    @Override
                    public ClickLog map(String value) throws Exception {
                        return JSON.parseObject(value, ClickLog.class);
                    }
                });
        System.out.println("dataSet01条目数是：" + dataSet01.count());
        System.out.println("dataSet02条目数是：" + dataSet02.count());
        UnionOperator<ClickLog> unionDataSet = dataSet01.union(dataSet02);
        System.out.println("union 之后条目数是：" + unionDataSet.count());


        System.out.println("=================");
        //TODO:9、【distinct函数】对数据进行去重操作
        DistinctOperator<ClickLog> distinctDataSet = unionDataSet.distinct("browserType");
        System.out.println(distinctDataSet.count());
        DistinctOperator<ClickLog> distinctDataSet02 = unionDataSet.distinct();
        System.out.println(distinctDataSet02.count());


    }


}

