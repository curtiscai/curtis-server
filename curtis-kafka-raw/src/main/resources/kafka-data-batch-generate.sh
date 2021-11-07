#!/bin/sh

# 如果主题存在则先删除主题
# kafka-topics.sh --delete --bootstrap-server node100:9092 --topic topic-001

# 响应Ctrl+C中断
trap 'onCtrlC' INT
function onCtrlC () {
    echo 'Ctrl+C is captured'
    exit 1
}

# kafka所在目录
kafka_path=/opt/app/kafka
# broker
broker_list=node100:9092,node102:9092,node103:9092
# kafka的topic
topic=topic-001
# 消息总数
total_num=10000
# 一次批量发送的消息数
batch_num=100
# 该标志为true，表示文件中的第一条记录
first_line_flag='true'

# 创建Topic
${kafka_path}/bin/kafka-topics.sh --create --bootstrap-server ${broker_list} --topic ${topic} --partitions 3 --replication-factor 2

echo "start send message at $(date "+%Y-%m-%d %H:%M:%S")"

for ((i=1; i<=total_num; i ++))
do
	# 消息内容，请按照实际需要自行调整
  message_content=batch_message-${i}-$(date "+%Y-%m-%d %H:%M:%S")

  # 如果是每个批次的第一条，就要将之前的内容全部覆盖，如果不是第一条就追加到尾部
  if [ 'true' == ${first_line_flag} ] ; then
    echo "${message_content}" > batch_message.txt

    # 将标志设置为false，这样下次写入的时候就不会清理已有内容了
    first_line_flag='false'
  else
    echo "${message_content}" >> batch_message.txt
  fi

  # 取余数
  mod_val=$(( ${i} % ${batch_num} ))

  # 如果达到一个批次，就发送一次消息
  if [ ${mod_val} = 0 ] ; then
    # 在控制台显示进度
    echo "${i} of ${total_num} sent"

    # 批量发送消息，并且将控制台返回的提示符重定向到/dev/null
    cat batch_message.txt | ${kafka_path}/bin/kafka-console-producer.sh --broker-list ${broker_list} --topic ${topic} | > /dev/null

    # 将标志设置为true，这样下次写入batch_message.txt时，会将文件中的内容先清除掉
    first_line_flag='true'
  fi
done

echo "end send message at $(date "+%Y-%m-%d %H:%M:%S")"