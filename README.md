# 基于GPU的Flush操作优化

## 目的

**实验目的：** 在leveldb数据库中，需要将内存中MemTable格式的数据转化到硬盘中以SST文件格式存储，该过程称为Flush操作，实验目的就是加快该转换操作。

## 基本流程

**主要思想：** 每个block负责一个datablock，每个thread负责一个kvgroup

**串行数据流：**
db_impl::WriteLevel0Table->builder::BuildTable->table_builder::Add->block_builder::Add

**并行数据流：**
db_impl::WriteLevel0Table->builder::BuildTable->table_builder::Add_Accelerate->table_builder_accelerate::Calculate_TablesContents->table_builder_accelerate::Add_Accelerate_Implement_Multiple_Datablock

**注1：** SST文件格式包括data block、filter block、metaindex block等部分，由于data block是核心操作，所以本实验仅优化data block部分。

**注2：** CUDA代码位于accelerate目录下

## 程序测试

**本实验使用cmake后，build目录下的level_tests进行测试**

**实验结果：** 并行程序时间开销略大于串行程序，暂无性能提升。

**结果分析：**

* 在level_tests测试中，每次转换时kv数目非常少，且单个kv的大小非常大

* SST文件中其它部分的串行执行影响性能提升




