package com.test.flink.other;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @Author: Jface
 * @Date: 2021/9/6 14:41
 * @Desc: Flink 批处理中分布式缓存：将小文件数据进行缓存
 */
public class BatchDistributedCacheDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source
        // 大表数据
        DataSource<Tuple3<Integer, String, Integer>> scoreDataSet = env.fromCollection(
                Arrays.asList(
                        Tuple3.of(1, "语文", 50),
                        Tuple3.of(1, "数学", 70),
                        Tuple3.of(1, "英语", 86),
                        Tuple3.of(2, "语文", 80),
                        Tuple3.of(2, "数学", 86),
                        Tuple3.of(2, "英语", 96),
                        Tuple3.of(3, "语文", 90),
                        Tuple3.of(3, "数学", 68),
                        Tuple3.of(3, "英语", 92)
                )
        );
        // TODO:将数据文件进行缓存，不能太大，属于小文件数据
        env.registerCachedFile("datas/distribute_cache_student", "cache_students");

        //3.转换操作-transformation,使用广播变量值，使用 RickMapFunction 函数，
        MapOperator<Tuple3<Integer, String, Integer>, String> resultDataSet = scoreDataSet.map(new RichMapFunction<Tuple3<Integer, String, Integer>, String>() {
            //TODO: 定义Map集合，存储文件中的数据
            private HashMap<Integer, String> stuMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO: 获取分布式缓存文件数据
                File file = getRuntimeContext().getDistributedCache().getFile("cache_students");
                //TODO: 获取文件中的数据,放入 Map集合
                List<String> list = FileUtils.readLines(file);//一个元素就是一条数据
                for (String line : list) {
                    if (null != line && !"".equals(line)) {
                        String[] arr = line.trim().split(",");
                        stuMap.put(Integer.valueOf(arr[0]), arr[1]);
                    }
                }
            }

            @Override
            public String map(Tuple3<Integer, String, Integer> value) throws Exception {
                Integer studentId = value.f0;
                String subjectName = value.f1;
                Integer score = value.f2;
                String studentName = stuMap.get(studentId);
                return studentName + "," + subjectName + "," + score;
            }
        });
        //4. 数据终端-sink
        resultDataSet.printToErr();
    }
}
