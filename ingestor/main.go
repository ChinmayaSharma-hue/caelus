package ingestor

import (
	"github.com/ChinmayaSharma-hue/caelus/core/config"
)

func main() {
	// make an api call to the source API
	// what should be configurable?
	// ingestionSourceType: gmail for now
	// ingestionSourceFilters: duration, mailing list
	// rate limit is set for gmail API so spawn as many goroutines as feasible
	// get a list of messages for the day in one go, and spawn goroutines to get each different sets
	// in each goroutine that gets a set of messages, it should do the following,
	// push metadata in a bulk insert to the metadata DB
	// get an embedding for each of the messages
	// push the embedding to the vector DB
	config, err := config.N

	// division of functionalities
	// communication with source: get a list of mails, get each mail
	// ingestion manager that does the following,
	// use the source to ingest metadata
	// push the generic metadata in a bulk insert to the database
	// spawn goroutines to ingest actual data using source and push the data to the vector DB using source
}
