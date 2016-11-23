Checkpoint Tool
===============

Allows to examine/modify Samza job checkpoints.


Running
=======

To run/build you'll need Go 1.6+ and Glide (optionally).

To run: 
```
glide install
go run checkpoint_tool.go
```

To build:
```
glide install
go build checkpoint_tool.go
```

##Arguments

Argument           | Required | Description
-------------------|----------|------------
-brokers string    | yes      | The Kafka brokers to connect to (a comma separated list)
-job string        | yes      | Samza job name
-file string       | yes      | Checkpoints file path
-commit            | no       | Commit data to file/topic, otherwise just print result
-only string       | no       | Include only these streams from input source (a comma separated list)
-except string     | no       | Exclude streams from input source (a comma separated list)
-extract           | no       | Extract checkpoints from topic to file
-replace           | no       | Replace checkpoints in topic with data from file
-patch             | no       | Patch (merge) checkpoints in topic with data from file
-verbose           | no       | More logging

###Examples

Extract all checkpoints from topic to file:
```
go run checkpoint_tool.go -brokers kafka-1:9092,kafka-2:9092 -job some-job -extract -file some-job.json -commit

```

Extract only checkpoints for streams `xxx` and `yyy` to file:
```
go run checkpoint_tool.go -brokers kafka-1:9092,kafka-2:9092 -job some-job -extract -file some-job.json -only xxx,yyy -commit

```

Replace all checkpoints except stream `xxx` (completely remove stream offsets!) from file:
```
go run checkpoint_tool.go -brokers kafka-1:9092,kafka-2:9092 -job some-job -replace -file some-job.json -except xxx -commit

```

Patch checkpoints except stream `xxx` (leave as is!) from file:
```
go run checkpoint_tool.go -brokers kafka-1:9092,kafka-2:9092 -job some-job -patch -file some-job.json -except xxx -commit

```
