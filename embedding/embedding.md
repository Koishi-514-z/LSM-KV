# Embedding

**在本次实验中, 你不需要写任何代码, 只需要成功编译并探究一些问题**

## LLAMA.CPP

**LLAMA.cpp** 是一个基于 C/C++ 的高性能推理框架, 专门用于在本地 CPU/GPU 上高效运行 **Meta（原 Facebook）的 LLaMA 系列大语言模型（LLM）** 及其衍生模型（如 Alpaca、Vicuna、Mistral 等）。它采用 **量化技术**（如 4-bit、5-bit、8-bit 等）来减少模型大小和内存占用, 同时保持较高的推理速度。

**LLAMA.cpp** 需要使用gguf格式的模型, 我们已经为你提供了一个gguf格式的模型, 你可以在 `model` 目录下找到。

## TASK

### add submodule for LLAMA.CPP

```bash
git init (如果git已初始化可以跳过)
git submodule add https://gitee.com/ShadowNearby/llama.cpp.git third_party/llama.cpp
```

### 编译LLAMA.CPP

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

### 运行测试

```bash
./build/test/Embedding_Test
```

### 探究

首先, 你应该根据源代码和输出的 LOG 探究, 在运行测试的过程中, 整个程序都发生了什么？(**但是不要深入 `LLAMA.CPP` 的源代码内部**)

以下这些测试包含的 **语义** 是什么？

```cpp
if (sim_matrix["Apple"]["Banana"] > sim_matrix["Apple"]["Man"]) {
  passed_count++;
}
if (sim_matrix["Apple"]["Orange"] > sim_matrix["Apple"]["Chicken"]) {
  passed_count++;
}
if (sim_matrix["Banana"]["Orange"] > sim_matrix["Banana"]["Man"]) {
  passed_count++;
}
```

```cpp
if (max_sim_sentence != supposed_sentence) {
  std::cerr << "Failed to find the correct sentence" << std::endl;
  return passed_count;
}
passed_count++;
```

**再次强调：不要深入 `LLAMA.CPP` 的源代码内部, 内部非常复杂**

### 优化(不计入成绩)

1. 如果你有 `NVIDIA GPU`, 可以使用 `CUDA` 加速 `LLAMA.CPP`, 对比与 `CPU` 版本的运行时间
2. 修改配置参数, 例如：
    - `MODEL`
    - `NGL`
    - `BATCH_SIZE`
    - `CONTEXT_SIZE`
  对比修改前后的运行时间, 探究这些参数起到了什么作用

### Others(不计入成绩)

1. 根据 `LLAMA.CPP` 的 `third_party/llama.cpp/examples/simple-chat` 探索 `Chat` 模式, 选择一个参数量较小的模型, 运行 `Chat` 模式, 并测试其效果
2. 将 `test` 中的 `Embedding_Test` 中的上下文作为 `Chat` 模式的输入, 找到合适的 `prompt`, 运行 `Chat` 模式, 并测试其效果。

## handout

你只需要提交一个 `pdf` 文档, 包含探究部分你的回答。

报告应为 `pdf` 格式, 命名为 `学号_姓名.pdf`, 例如 `523030912345_张三.pdf`
