## Just a RAG pipeline application
Does the following,
1. Ingest data from configured sources into a vector database
2. Construct prompts using the data stored in the vector database
3. Get response from the configured model for the prompts

#### Instructions
1. Build and run `docker-compose.yaml` to run all the dependent services
2. Before building the ingestor, processor and feeder images,
    1. Have a configuration file in the format of `sample-config.yaml`
    2. Have a prompt file in the root directory that describes what you would like to do with the data
3. Build and run the ingestor, processor and feeder images.