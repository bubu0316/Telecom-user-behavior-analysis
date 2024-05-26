# Telecom-user-behavior-analysis
### 项目介绍
- 通过模拟数据（上网用户ip，访问的网站，上网模式，用户所在城市，上网使用的设备，上网的时间）对数据仓库进行设计和建设
### 项目步骤
- Hadoop基本环境搭建 大数据环境的统一配置, HDSF分布式存储环境搭建, Yarn分布式任务调度
- MySQL数据库的安装和应用
- SpringBoot项目的日志生成生成程序的流程分析
- Flume日志采集到HDFS,并且完成shell脚本的编写
- Hive-on-Spark环境的搭建
- Hive的基本操作, 外部表,分区表的创建,表数据的导入
- 数据仓库的分层架构ODS --> DWD --> DWS --> ADS 数仓结构设计
- DataX技术从ADS同步数据到MySQL数据库
- QuickBI完成数据可视化处理
### 环境搭建
#### 使用biz01, hadoop01, hadoop02, hadoop03 三台linux系统的虚拟机进行搭建
##### ip设置
- localhost 192.168.66.1
- biz01 192.168.66.100
- hadoop01 192.168.66.101
- hadoop02 192.168.66.102
- hadoop03 192.168.66.109
##### 集群规划
- biz01 : jdk1.8 mysql flume
- hadoop01 : jdk1.8 namenode,datanode resourcemanager,nodemanager
- hadoop02 : jdk1.8 datanode,secondarynamenode nodemanager historyserver
- hadoop03 : jdk1.8 datanode nodemanager spark hive datax
### 文件说明
- env/custom_env.sh    环境变量配置，路径为/etc/profile.d/
- files    一些安装hadoop,hive,spark等软件后需要自行更改的配置文件
- json     从hdfs的数据导出到本地数据库的DataX的jobs
- result    数据可视化结果展示，使用的是阿里云的QuickBI
- screenshot     阶段性成果截图展示 datagrip.png : hiveserver2连接datagrip数据库展示    hdfs.png : hdfs的根目录文件展示
- sql-code      数据仓库建模的sql代码
- src      ODS层到DWD层的数据处理使用的java脚本
