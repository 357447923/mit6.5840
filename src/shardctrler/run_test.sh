#!/bin/bash
#
## 设置执行次数
COUNT=10

echo "开始连续执行 $COUNT 次 go test..."

for ((i=1; i<=$COUNT; i++))
do
     echo "=== 第 $i 次执行 ==="
         go test
         echo "=== 第 $i 次执行完成 ==="
 	 echo ""
done

echo "已完成 $COUNT 次 go test 执行"
