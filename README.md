# gc
模拟FAWN garbage collection操作
实现两种不同的gc模式：
  1.普通gc模式，对log文件进行compact操作，不留空隙的凑紧文件。
  2.优化gc模式，留少量空隙，快级别对齐，给deduplication应用制造机会。
