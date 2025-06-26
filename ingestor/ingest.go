package ingestor

import (
	"context"
	"github.com/ChinmayaSharma-hue/caelus/core/source"
)

func ingest(ctx context.Context, source source.Source, metadataList []source.Metadata) error {
	// push metadata in a bulk insert to the metadata DB
	// get an embedding for each of the messages
	// push the embedding to the vector DB

	_, err := source.GetData(metadataList)
	if err != nil {
		return err
	}

	return nil
}
