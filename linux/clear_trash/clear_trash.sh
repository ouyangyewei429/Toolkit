#!/bin/bash
source /etc/profile;

# ----------------------------
#
# Title:    清理指定目录的垃圾文件
# Author:   ouyangyewei
#
# Update:   ouyangyewei, 2016/04/18, 清理指定HDFS目录的垃圾文件
# Update:   ouyangyewei, 2016/05/06, 修复本地目录未清理的bug
# Update:   ouyangyewei, 2016/07/10, 改进ARES本地文件清理逻辑，将文件夹删除
# Update:   ouyangyewei, 2016/07/18, 更改删除频率，更改为删除2天前的历史数据
# Update:   ouyangyewei, 2016/08/09, 配置某些特殊目录不删除
# Update:   ouyangyewei, 2016/09/29, 配置删除hive线上临时目录
# Update:   ouyangyewei, 2016/10/09, 重写、优化删除HDFS历史文件脚本
# Update:   ouyangyewei, 2017/02/10, 修改删除HDFS历史数据脚本，空目录也占用inode，需要清空
# Update:   ouyangyewei, 2017/02/10, 移除删除xxx@xxxx形式的逻辑
# Update:   ouyangyewei, 2017/02/10, 优化删除本地文件清理逻辑，先删除历史文件，再删除空文件夹，逻辑更加清晰
#
# ---------------------------

# 指定删除多少天之前的历史数据
N_DAYS=2

# 特殊目录不删除
safe_dirs=(
/home/map/schedule/data/buffet/ares/36428
/home/etl/schedule/data/buffet/ares/50414
)

# 目标清理目录
target_dirs=(
/home/map/schedule/aresdata
/home/map/schedule/data/buffet/ares
/home/etl/schedule/data/buffet/ares
/home/map/schedule/data/indexpool
/home/map/quidditch/quidditch/quidditch/logs
)

#####################################################
# Check if string contains substring
#
# Parameters:
#     string to match
#
# Returns:
#     1 if contains substring, or 0
#####################################################
function is_contain() {
  target=$1
  for safe_dir in ${safe_dirs[@]}; do
    [[ "$target" == *"$safe_dir"* ]] && { echo 1; exit 0; }
  done

  echo 0;
}

#####################################################
# 清理指定的目标目录的历史文件，
# 不清理safe_dirs指定的目录文件
#####################################################
function clear_local_history_file() {
  for target_dir in ${target_dirs[@]}; do
    # remove history file
    file_list=`find $target_dir -type f -mtime +${N_DAYS}`
    for file_path in $file_list; do
      [[ $(is_contain $file_path) -eq 0 ]] && rm -rfv $file_path
    done

    # remove empty directory
    find $target_dir -type d -empty | xargs rm -rfv
  done
  echo "Success to clear local history file..."
}


#####################################################
# 清理/app/lbs/nuomi-da-stat队列的历史文件
#####################################################
function clear_hdfs_history_file() {
  hadoop fs -lsr /app/lbs/nuomi-da-stat | awk -v n_days=${N_DAYS} '
    BEGIN {
      IFS="\t";
      n_days_ago=strftime("%F", systime()-n_days*24*3600)
    } {
      if (substr($1,1,1)=="-" && $6<n_days_ago) {print "hadoop fs -Dhadoop.job.ugi=lbs-stat,aresisperfect -rmr", $8}
    }' | /bin/bash
  echo "Success to clear hdfs history file..."
}


# ---------------------------

clear_local_history_file &&
clear_hdfs_history_file &
clear_hdfs_history_file &
clear_hdfs_history_file
