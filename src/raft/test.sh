#!/bin/bash

# 设置变量
count=1
max_attempts=20

# 循环执行go test命令，最多尝试20次
while [ $count -le $max_attempts ]; do
    echo "Running go test -run 2A, Attempt: $count"
    
    # 执行go test命令并检查输出是否包含FAIL字符串
    output=$(go test -run 2A)
    if [[ $output == *FAIL* ]]; then
        echo "Test failed. Exiting."
        exit 1
    fi

    # 增加尝试计数
    ((count++))

    # 暂停一段时间，可以根据需要调整
    sleep 1
done

echo "Max attempts reached. Test did not fail."
exit 0

