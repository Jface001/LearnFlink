package com.test.flink.table.batch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: Jface
 * @Date: 2021/9/14 13:52
 * @Desc: 定义实体类，用于封装 Wordcount数据
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WordCount {
    private String word;
    private Long counts;
}
