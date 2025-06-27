package main

func main() {
	// functionality of the processor is the construction of prompts and pushing it to the postgres queue

	// a persistent queue storing IDs, the IDs pushed during to the queue during ingestion
	// the processor must have several workers doing the following
	// pick an ID from the queue
	// get top 50 data points from the vector store
	// pick the top x such that the sum of the sizes of the data is closest to the cutoff, if the closeness is lower than a threshold, skip this ID
	// mark all the x data points as consumed in the vector DB
	// construct a prompt from the x data points and push it to the persistent queue
}
