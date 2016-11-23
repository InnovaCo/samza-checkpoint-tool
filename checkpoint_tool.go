package main

import (
	"github.com/Shopify/sarama"

	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"fmt"
	"bufio"
)

type Key struct {
	GrouperFactory	string	`json:"systemstreampartition-grouper-factory"`
	TaskName	string	`json:"taskName"`
	Type		string	`json:"type"`
}

type Checkpoint struct {
	System		string	`json:"system"`
	Partition	string	`json:"partition"`
	Offset		string	`json:"offset"`
	Stream		string	`json:"stream"`
}

type PartitionStreams map[string]Checkpoint

type Checkpoints map[Key]PartitionStreams

var (
	brokers   = flag.String("brokers", os.Getenv("KAFKA_BROKERS"), "The Kafka brokers to connect to, as a comma separated list")
	verbose   = flag.Bool("verbose", false, "More logging")
	jobName	  = flag.String("job", "", "Samza job name")
	extract	  = flag.Bool("extract", false, "Extract checkpoints from topic to file")
	replace   = flag.Bool("replace", false, "Replace checkpoints in topic with data from file")
	patch	  = flag.Bool("patch", false, "Patch (merge) checkpoints in topic with data from file")
	include	  = flag.String("only", "", "Include only these streams from input source (a comma separated list)")
	exclude	  = flag.String("except", "", "Exclude streams from input source (a comma separated list)")
	commit 	  = flag.Bool("commit", false, "Commit data to file/topic, otherwise just print result")
	filePath  = flag.String("file", "", "Checkpoints file path")
)

func main() {
	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	checkDefault(*brokers)
	checkDefault(*jobName)
	checkDefault(*filePath)

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	if (*commit) {
		log.Println("WARNING! This run will result in persistent changes!")
	} else {
		log.Println("WARNING! This is a TEST run. No changes will be commited to file/topic.")
	}

	client := buildClient(brokerList)
	defer client.Close()

	producer := buildProducer(client)
	defer producer.Close()

	consumer := buildConsumer(client)
	defer consumer.Close()

	includeList := filterEmpty(strings.Split(*include, ","))
	excludeList := filterEmpty(strings.Split(*exclude, ","))

	topic := fmt.Sprintf("__samza_checkpoint_ver_1_for_%s_1", *jobName)

	validateCheckpointTopic(topic, client)

	if *extract {
		checkpoints := readCheckpointsFromTopic(topic, consumer)
		filtered := filterCheckpoints(checkpoints, includeList, excludeList)

		if *commit {
			writeCheckpointsToFile(filtered, *filePath)
			log.Printf("Successfully extracted %s to %s\n", topic, *filePath)
		} else {
			printCheckpoints(filtered)
		}
		os.Exit(0)
	}

	if *replace {
		checkpoints := readCheckpointsFromFile(*filePath)
		filtered := filterCheckpoints(checkpoints, includeList, excludeList)

		if *commit {
			writeCheckpointsToTopic(filtered, topic, producer)
			log.Printf("Successfully replaces %s with %s\n", topic, *filePath)
		} else {
			printCheckpoints(filtered)
		}

		os.Exit(0)
	}

	if *patch {
		checkpoints := readCheckpointsFromTopic(topic, consumer)

		patch := readCheckpointsFromFile(*filePath)
		filteredPatch := filterCheckpoints(patch, includeList, excludeList)

		patched := mergeCheckpoints(checkpoints, filteredPatch)

		if *commit {
			writeCheckpointsToTopic(patched, topic, producer)
			log.Printf("Successfully patched %s with %s\n", topic, *filePath)
		} else {
			printCheckpoints(patched)
		}

		os.Exit(0)
	}

	flag.PrintDefaults()
	os.Exit(1)
}

func filterCheckpoints(checkpoints Checkpoints, include []string, exclude []string) Checkpoints {
	filtered := make(Checkpoints)

	for key := range checkpoints {

		streams := checkpoints[key]
		filteredStreams := make(PartitionStreams)

		for streamKey := range streams {
			stream := streams[streamKey]
			streamTopic := stream.Stream
			if hasOrEmpty(streamTopic, include) && hasNoOrEmpty(streamTopic, exclude) {
				filteredStreams[streamKey] = stream
			}
		}

		filtered[key] = filteredStreams
	}

	return filtered
}

func mergeCheckpoints(checkpoints Checkpoints, patch Checkpoints) Checkpoints {
	merged := cloneCheckpoints(checkpoints)

	for key := range patch {

		patchStreams := patch[key]
		if len(patchStreams) == 0 {
			continue
		}

		_, partitionExists := checkpoints[key]
		if partitionExists {
			for streamKey, checkpoint := range patchStreams {
				merged[key][streamKey] = checkpoint
			}
		} else {
			merged[key] = patchStreams
		}
	}

	return merged
}

func writeCheckpointsToTopic(checkpoints Checkpoints, topic string, producer sarama.SyncProducer) {
	for key, streams := range checkpoints {
		serializedKey, err := json.Marshal(key)
		check(err)
		serializedStreams, err := json.Marshal(streams)
		check(err)

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.ByteEncoder(serializedKey),
			Value: sarama.ByteEncoder(serializedStreams),
		}

		log.Printf("Producing %s: %s\n", string(serializedKey), string(serializedStreams))

		_, _, err = producer.SendMessage(msg)

		if err != nil {
			log.Fatalf("Unable to produce to %s: %s\n", topic, err)
		}
	}
}

func readCheckpointsFromFile(path string) Checkpoints {
	checkpoints := make(Checkpoints)

	f, err := os.Open(path)
	check(err)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		kv := strings.Split(scanner.Text(), "\t")

		key, err := unmarshallKey([]byte(kv[0]))
		if err != nil {
			log.Fatalf("Unexpected key '%s': %s\n", string(kv[0]), err)
			continue
		}

		streams, err := unmarshallStreams([]byte(kv[1]))
		if err != nil {
			log.Fatalf("Unexpected streams '%s': %s\n", string(kv[1]), err)
			continue
		}

		checkpoints[key] = streams
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return checkpoints
}

func writeCheckpointsToFile(checkpoints Checkpoints, path string) {
	f, err := os.Create(path)
	check(err)
	defer f.Close()

	for key := range checkpoints {
		serializedKey, err := json.Marshal(key)
		check(err)
		serializedCheckpoint, err := json.Marshal(checkpoints[key])
		check(err)
		_, err = f.WriteString(fmt.Sprintf("%s\t%s\n", serializedKey, serializedCheckpoint))
		check(err)
	}
}

func printCheckpoints(checkpoints Checkpoints) {
	for key := range checkpoints {
		serializedKey, err := json.Marshal(key)
		check(err)
		serializedCheckpoint, err := json.Marshal(checkpoints[key])
		check(err)
		fmt.Printf("%s\t%s\n", serializedKey, serializedCheckpoint)
	}
}

func readCheckpointsFromTopic(topic string, consumer sarama.Consumer) Checkpoints {
	c, _ := consumer.ConsumePartition(topic, 0, 0)

	checkpoints := make(Checkpoints)

	for message := range c.Messages() {
		fmt.Printf("Restoring from topic: %d/%d (%d left)          \r",
			message.Offset, c.HighWaterMarkOffset(), c.HighWaterMarkOffset() - message.Offset)

		key, err := unmarshallKey(message.Key)
		if err != nil {
			//fmt.Printf("Unexpected key '%s': %s\n", string(message.Key[:]), err)
			continue
		}

		streams, err := unmarshallStreams(message.Value)
		if err != nil {
			//fmt.Printf("Unexpected message '%s': %s\n", string(message.Value[:]), err)
			continue
		}

		checkpoints[key] = streams

		if message.Offset + 1 >= c.HighWaterMarkOffset() {
			break
		}
	}

	return checkpoints
}

func unmarshallKey(raw []byte) (Key, error) {
	var key Key
	err := json.Unmarshal(raw[:], &key)
	return key, err
}

func unmarshallStreams(raw []byte) (PartitionStreams, error) {
	var streams PartitionStreams
	err := json.Unmarshal(raw[:], &streams)
	return streams, err
}

func validateCheckpointTopic(topic string, client sarama.Client) {
	log.Println("Validating topic:", topic)

	client.RefreshMetadata(topic)
	partitions, err := client.Partitions(topic)
	if err != nil {
		log.Fatalf("Unable to get info for topic %s: %s", topic, err)

	}
	if len(partitions) != 1 {
		log.Fatalf("Invalid partitions count for %s, expected 1, got %d", topic, len(partitions))
	}
}

func buildClient(brokerList []string) sarama.Client {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionNone
	//config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Producer.Flush.Bytes = 1
	config.Producer.Return.Successes = true

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		panic(err)
	} else {
		log.Println("Connected.")
	}

	return client
}

func buildProducer(client sarama.Client) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatalln("Failed to start producer:", err)
	}
	return producer
}

func buildConsumer(client sarama.Client) sarama.Consumer {

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalln("Failed to start consumer:", err)
	}

	return consumer
}

func check(err error) {
	if err != nil {
		log.Fatalln("Unexpected error: ", err)
	}
}

func checkDefault(value string) {
	if value == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func has(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func hasOrEmpty(a string, list []string) bool {
	return len(list) == 0 || has(a, list)
}

func hasNoOrEmpty(a string, list []string) bool {
	return len(list) == 0 || !has(a, list)
}

func filterEmpty (s []string) []string {
	var r []string
	for _, str := range s {
		if str != "" {
			r = append(r, str)
		}
	}
	return r
}

func cloneCheckpoints(source Checkpoints) Checkpoints {
	out := make(Checkpoints, len(source))

	for key, streams := range source {
		outStreams := make(PartitionStreams, len(streams))
		for stream, checkpoint := range streams {
			outStreams[stream] = checkpoint
		}
		out[key] = outStreams
	}
	return out
}