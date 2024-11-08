package block

import (
	"encoding/json"
	"fmt"

	"github.com/kalyan3104/k-chain-core-go/core/check"
	coreData "github.com/kalyan3104/k-chain-core-go/data"
	"github.com/kalyan3104/k-chain-core-go/data/block"
	"github.com/kalyan3104/k-chain-es-indexer-go/data"
	"github.com/kalyan3104/k-chain-es-indexer-go/process/dataindexer"
	"github.com/kalyan3104/k-chain-es-indexer-go/process/elasticproc/converters"
)

// SerializeBlock will serialize a block for database
func (bp *blockProcessor) SerializeBlock(elasticBlock *data.Block, buffSlice *data.BufferSlice, index string) error {
	if elasticBlock == nil {
		return dataindexer.ErrNilElasticBlock
	}

	meta := []byte(fmt.Sprintf(`{ "index" : { "_index":"%s", "_id" : "%s" } }%s`, index, converters.JsonEscape(elasticBlock.Hash), "\n"))
	serializedData, errMarshal := json.Marshal(elasticBlock)
	if errMarshal != nil {
		return errMarshal
	}

	return buffSlice.PutData(meta, serializedData)
}

// SerializeEpochInfoData will serialize information about current epoch
func (bp *blockProcessor) SerializeEpochInfoData(header coreData.HeaderHandler, buffSlice *data.BufferSlice, index string) error {
	if check.IfNil(header) {
		return dataindexer.ErrNilHeaderHandler
	}

	metablock, ok := header.(*block.MetaBlock)
	if !ok {
		return fmt.Errorf("%w in blockProcessor.SerializeEpochInfoData", dataindexer.ErrHeaderTypeAssertion)
	}

	epochInfo := &data.EpochInfo{
		AccumulatedFees: metablock.AccumulatedFeesInEpoch.String(),
		DeveloperFees:   metablock.DevFeesInEpoch.String(),
	}

	id := header.GetEpoch()
	meta := []byte(fmt.Sprintf(`{ "index" : { "_index":"%s", "_id" : "%d" } }%s`, index, id, "\n"))
	serializedData, errMarshal := json.Marshal(epochInfo)
	if errMarshal != nil {
		return errMarshal
	}

	return buffSlice.PutData(meta, serializedData)
}
