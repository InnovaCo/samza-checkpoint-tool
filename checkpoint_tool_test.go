package main

import (
	"testing"
	"fmt"

	"github.com/stretchr/testify/assert"
)

func createKey(partition string) Key {
	return Key {
		GrouperFactory: "org.apache.samza.container.grouper.stream.GroupByPartitionFactory",
		TaskName: fmt.Sprint("Partition-", partition),
		Type: "checkpoint",
	}
}

func createCheckpoint(stream string, partition string, offset string) Checkpoint {
	return Checkpoint {
		System: "kafka",
		Partition: partition,
		Offset: offset,
		Stream: stream,
	}
}

func createCheckpoints() Checkpoints {
	checkpoints := make(Checkpoints)

	streams := make(PartitionStreams)
	streams["topic abc 121"] = createCheckpoint("abc", "121", "11111")
	streams["topic def 121"] = createCheckpoint("def", "121", "66666")
	streams["topic xyz 121"] = createCheckpoint("xyz", "121", "77777")
	checkpoints[createKey("121")] = streams

	streams = make(PartitionStreams)
	streams["topic abc 13"] = createCheckpoint("abc", "13", "22222")
	streams["topic def 13"] = createCheckpoint("def", "13", "55555")
	streams["topic xyz 13"] = createCheckpoint("xyz", "13", "33333")
	checkpoints[createKey("13")] = streams

	return checkpoints
}

func TestHas(t *testing.T) {
	array := []string{"xxx", "YYY", "zzz"}
	empty := []string{}

	assert.False(t, has("ololo", array), "Must check if element doesn't exist")
	assert.True(t, has("xxx", array), "Must check if element exists")
	assert.False(t, has("yyy", array), "Must use case sensitive checks")
	assert.False(t, has("xxx", empty), "Must not find anything in empty")
}

func TestHasOrEmpty(t *testing.T) {
	array := []string{"xxx", "YYY", "zzz"}
	empty := []string{}

	assert.False(t, hasOrEmpty("ololo", array), "Must return false when element doesn't exist")
	assert.True(t, hasOrEmpty("xxx", array), "Must return true when element exists")
	assert.True(t, hasOrEmpty("xxx", empty), "Must return true when slice is empty")
}

func TestFilteringEmpty(t *testing.T) {
	initial := createCheckpoints()
	filtered := filterCheckpoints(initial, []string{}, []string{})

	assert.Equal(t, filtered, initial, "Empty filters should not alter checkpoints")
}

func TestFilteringIncludeExclude(t *testing.T) {
	initial := createCheckpoints()
	filtered := filterCheckpoints(initial, []string{ "abc", "def"}, []string{ "abc" })

	assert.Equal(t, len(filtered), 2, "Must keep all partitions")

	streams := filtered[createKey("121")]
	assert.NotContains(t, streams, "topic abc 121", "Must allow only included topics")
	assert.Contains(t, streams, "topic def 121", "Must allow only included topics")
	assert.NotContains(t, streams, "topic xyz 121", "Must allow only included topics")

	streams = filtered[createKey("13")]
	assert.NotContains(t, streams, "topic abc 13", "Must allow only included topics")
	assert.Contains(t, streams, "topic def 13", "Must allow only included topics")
	assert.NotContains(t, streams, "topic xyz 13", "Must allow only included topics")
}

func TestMerging(t *testing.T) {
	initial := createCheckpoints()
	patch := make(Checkpoints)
	streams := make(PartitionStreams)
	streams["topic def 121"] = createCheckpoint("def", "121", "66666")
	streams["topic qrs 121"] = createCheckpoint("qrs", "121", "99999")
	patch[createKey("121")] = streams

	streams = make(PartitionStreams)
	streams["topic abc 67"] = createCheckpoint("abc", "67", "54321")
	streams["topic def 67"] = createCheckpoint("def", "67", "65432")
	patch[createKey("67")] = streams

	merged := mergeCheckpoints(initial, patch)

	assert.Equal(t, len(merged), 3, "Must merge all partitions")

	streams = merged[createKey("121")]
	assert.Contains(t, streams, "topic abc 121", "Must not remove existing streams")
	assert.Contains(t, streams, "topic def 121", "Must not remove existing streams")
	assert.Contains(t, streams, "topic xyz 121", "Must not remove existing streams")
	assert.Contains(t, streams, "topic qrs 121", "Must add new streams")

	streams = merged[createKey("13")]
	assert.Contains(t, streams, "topic abc 13", "Must not remove existing streams")
	assert.Contains(t, streams, "topic def 13", "Must not remove existing streams")
	assert.Contains(t, streams, "topic xyz 13", "Must not remove existing streams")

	streams, exists := merged[createKey("67")]
	assert.True(t, exists, "Must add new partitions")
	assert.Contains(t, streams, "topic abc 67", "Must add new streams")
	assert.Contains(t, streams, "topic def 67", "Must add new streams")
}


func TestUnmarshalling(t *testing.T) {
	rawKey := `
	{
		"systemstreampartition-grouper-factory":"org.apache.samza.container.grouper.stream.GroupByPartitionFactory",
		"taskName":"Partition 121",
		"type":"checkpoint"
	}`

	targetKey := Key{
		GrouperFactory: "org.apache.samza.container.grouper.stream.GroupByPartitionFactory",
		TaskName: "Partition 121",
		Type: "checkpoint",
	}

	rawStreams := `
	{
		"SystemStreamPartition [kafka, aion-events, 121]":{"system":"kafka","partition":"121","offset":"1354","stream":"aion-events"},
		"SystemStreamPartition [kafka, l2-events, 121]":{"system":"kafka","partition":"121","offset":"370","stream":"l2-events"},
		"SystemStreamPartition [kafka, achievements-out, 121]":{"system":"kafka","partition":"121","offset":"75450","stream":"achievements-out"}
	}
	`

	targetStreams := make(PartitionStreams)
	targetStreams["SystemStreamPartition [kafka, aion-events, 121]"] = Checkpoint{
		System: "kafka",
		Partition: "121",
		Offset: "1354",
		Stream: "aion-events",
	}
	targetStreams["SystemStreamPartition [kafka, l2-events, 121]"] = Checkpoint{
		System: "kafka",
		Partition: "121",
		Offset: "370",
		Stream: "l2-events",
	}
	targetStreams["SystemStreamPartition [kafka, achievements-out, 121]"] = Checkpoint{
		System: "kafka",
		Partition: "121",
		Offset: "75450",
		Stream: "achievements-out",
	}

	key, _ := unmarshallKey([]byte(rawKey))
	streams, _ := unmarshallStreams([]byte(rawStreams))

	assert.Equal(t, key, targetKey, "Must be able to unmarshall keys")
	assert.Equal(t, streams, targetStreams, "Must be able to unmarshall streams")
}

