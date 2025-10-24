#!/usr/bin/env bash
# Run ONLY the crash test for 6.824 MapReduce labs

# 可选：传入 "quiet" 将吞掉子进程输出
ISQUIET=$1
maybe_quiet() {
  if [ "$ISQUIET" == "quiet" ]; then
    "$@" > /dev/null 2>&1
  else
    "$@"
  fi
}

# （可选）-race；为避免 macOS Go<=1.17.5 的已知问题，这段与原脚本一致
RACE=
if [[ "$OSTYPE" = "darwin"* ]]; then
  if go version | grep 'go1.17.[012345]' > /dev/null 2>&1; then
    RACE=
    echo '*** Turning off -race due to known Mac go1.17.[0-5] issue'
  fi
fi

# 检测 timeout / gtimeout，并设置两个不同的超时窗口
TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1; then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1; then
    TIMEOUT=gtimeout
  else
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]; then
  TIMEOUT2="$TIMEOUT -k 2s 120s "
  TIMEOUT="$TIMEOUT -k 2s 45s "
fi

# 在全新目录中运行
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# 确保仅构建 crash 测试所需的二进制/插件
(
  cd ../../mrapps && go clean
  cd .. && go clean
) > /dev/null 2>&1

# 仅需这三个可执行文件 + 这两个插件
( cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go ) || exit 1
( cd ../../mrapps && go build $RACE -buildmode=plugin crash.go ) || exit 1
( cd .. && go build $RACE mrcoordinator.go ) || exit 1
( cd .. && go build $RACE mrworker.go ) || exit 1
( cd .. && go build $RACE mrsequential.go ) || exit 1

echo '***' Starting crash test.

# 生成正确答案：使用 nocrash 插件的顺序实现
../mrsequential ../../mrapps/nocrash.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

# 启动 coordinator（较长超时），写出一个完成标记文件以便 worker 循环退出
rm -f mr-done
((maybe_quiet $TIMEOUT2 ../mrcoordinator ../pg*txt); touch mr-done) &

# 给 coordinator 创建 Unix socket 的时间
sleep 1

# 启动若干会“崩溃”的 worker
maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so &

# 与 rpc.go 的 coordinatorSock() 一致的 sock 名
SOCKNAME=/var/tmp/5840-mr-`id -u`

# 循环拉起 worker，直到 coordinator 完成（mr-done 文件出现）或 sock 消失
(
  while [ -e "$SOCKNAME" -a ! -f mr-done ]; do
    maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done
) &

(
  while [ -e "$SOCKNAME" -a ! -f mr-done ]; do
    maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done
) &

while [ -e "$SOCKNAME" -a ! -f mr-done ]; do
  maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
  sleep 1
done

# 等待所有后台进程退出
wait

# 清理 socket（容错）
rm -f "$SOCKNAME"

# 校验输出
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt; then
  echo '---' crash test: PASS
  exit 0
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  exit 1
fi
