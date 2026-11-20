import json
import sys


executors = {}
tasks = {}
for line in sys.stdin:
    event = json.loads(line)
    if event.get("Event") == "SparkListenerExecutorAdded":
        executors[event["Executor ID"]] = event["Executor Info"]["Host"]
    if event.get("Event") == "SparkListenerTaskEnd" and event["Task End Reason"]["Reason"] == "Success":
        info = event["Task Info"]
        tasks[info["Host"]] = tasks.get(info["Host"], 0) + 1
assert executors, "В журнале нет исполнителей YARN"
assert tasks, "В журнале нет успешно выполненных задач"
print(json.dumps({"executors": executors, "successful_tasks_by_host": tasks}, indent=2))
