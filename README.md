# Project阶段2: Smart LSM
 
**强烈建议**：在开始实验之前，先**认真**阅读本文档，确保了解了实验的目的、内容和要求后再开始动手编写代码。

## Introduction

现代存储系统面临非结构化数据处理的重大挑战。**LSM-tree（Log-Structured Merge Tree** 作为高性能存储引擎，通过顺序写优化和层级合并机制，在键值存储领域占据重要地位。然而传统LSM-tree仅支持精确键值查询，难以应对**语义相似性搜索**需求。

本阶段将基于LSM-tree架构，通过集成**语义嵌入模型（Embedding Model）** 与 **近似最近邻搜索（ANN）** 算法，赋予存储引擎语义检索能力。

## Background

在电商推荐系统等典型场景中，商品特征常以高维向量形式存在。传统LSM-tree能高效存储键值对，但面对"查找与'水果'语义相似的商品"这类查询时，存在局限：**仅支持精确键匹配，无法处理语义相似性**。

在这个阶段，你将会为你的LSM-tree添加语义检索功能，设计实现一个 `Smart LSM-tree`。

## Process Overview

例如，往LSM-tree中插入KV对如下：
3 -> apple
4 -> banana
5 -> pear
6 -> sky
 ```
 
之后，搜索字符串 `"fruit"`最相近的3个向量。你的 `LSM-tree`预期返回 `3 -> apple`、`4 -> banana`、`5 -> pear`这三个KV对。
 
## Implementation Tips

为了支持语义检索，你在搜索过程中可以使用phase2 warmup阶段提供的embedding模型。`LSM-tree`中的 `Value`转化为向量。

对于向量的相似程度计算，你需要使用**余弦相似度**来计算两个向量之间的相似程度。

你可以使用以下公式来计算两个向量之间的余弦相似度：

$$
\text{cosine\_similarity}(A, B) = \frac{A \cdot B}{||A|| \cdot ||B||}
$$

其中，$A$和$B$是两个向量，$A \cdot B$表示它们的点积，$||A||$和$||B||$分别表示它们的模长。

## Task

- 在前一阶段LSM-tree的基础上，你需要实现接口 `std::vector<std::pair<std::uint64_t, std::string>> KVStore::search_knn(std::string query, int k)`，该接口接受一个查询字符串和一个整数 `k`，返回与查询字符串最相近的 `k`个向量的key和value。并且按照向量 `余弦相似度`从高到低的顺序排列。`E2E_test.cpp`不会因浮点数精度影响结果，之后的测试也会容忍一定的浮点数计算误差。
- 请注意LSMtree增删改查功能可能都要对应修改。
- 在这一阶段，你可以遍历LSM-tree中的KV对进行匹配，不需要考虑太多性能相关的问题。
- 在这一阶段，你需要保证key、value持久化，能通过上一阶段的 `persistence`和 `correctness`。但是这个阶段不需要考虑向量的持久化。
- 请把向量存储在内存中，如果每次KV操作都重新计算向量，性能会很差。

作为一个project，你只需要完成要求的接口以保证测试用例能够正常运行，我们不对你的实现细节要求做过多限制。

### Compare

请你对这次的系统运行进行分模块的计时分析，探索在程序中, 每个模块运行的时间(模块的划分由你来决定, **但是不用深入 `LLAMA.CPP` 的源代码内部**)。将以上内容写入到报告中。

你可以在每个部分开始前调用计时函数，在每个部分结束后调用计时函数，计算时间差，得到这部分的运行时间。

推荐使用 `std::chrono`库来实现计时功能。

### Test

在 `test`目录下有一个测试，供你调试自己的代码。

- `E2E_Test`：一个简单的端到端测试，测试你的整个系统是否正确。

## Build & Run

### Mac/Linux/Windows

请确保你的环境中已经安装了 `cmake`和C++编译器。

```
cmake -B build
cmake --build build --parallel
./build/test/...
```
 

## Report

你可以基于这个模板[report_template](https://latex.sjtu.edu.cn/4184116212ttcjdwbsfbgc#2b04b7)来撰写你的报告，详情参见链接里的模板内容。

报告应为pdf格式，命名为 `学号_姓名.pdf`，例如 `523030912345_张三.pdf`。会由canvas上单独的报告提交链接进行提交。

## Submission

请将你的代码打包成 `zip`格式，命名为 `学号_姓名.zip`，例如 `523030912345_张三.zip`

请不要上传你的 `build`、`data`、`model`、`test`目录，只需要上传你的项目的其他内容。