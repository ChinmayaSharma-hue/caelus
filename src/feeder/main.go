package main

func main() {
	// just reads from the buffer for prompts that need to be sent to the model
	// the pattern can be the same as processor, but for now one worker is sufficient, since we have to manage cost across workers
	// each worker does the following
	// picks a prompt ID from the buffer
	// gets the associated prompt from the promptStorage
	// sends the prompt to the model
	// in the response, checks the tokens used, and sends another prompt if it is only 50% of allowed token usage per day
}
