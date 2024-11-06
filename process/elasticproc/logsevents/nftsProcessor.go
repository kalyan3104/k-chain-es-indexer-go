package logsevents

import (
	"math/big"
	"time"

	"github.com/kalyan3104/k-chain-core-go/core"
	"github.com/kalyan3104/k-chain-core-go/core/sharding"
	coreData "github.com/kalyan3104/k-chain-core-go/data"
	"github.com/kalyan3104/k-chain-core-go/data/alteredAccount"
	"github.com/kalyan3104/k-chain-core-go/data/dcdt"
	"github.com/kalyan3104/k-chain-core-go/marshal"
	"github.com/kalyan3104/k-chain-es-indexer-go/data"
	"github.com/kalyan3104/k-chain-es-indexer-go/process/elasticproc/converters"
	logger "github.com/kalyan3104/k-chain-logger-go"
)

const (
	numTopicsWithReceiverAddress = 4
)

var log = logger.GetOrCreate("indexer/process/logsevents")

type nftsProcessor struct {
	pubKeyConverter          core.PubkeyConverter
	nftOperationsIdentifiers map[string]struct{}
	marshalizer              marshal.Marshalizer
}

func newNFTsProcessor(
	pubKeyConverter core.PubkeyConverter,
	marshalizer marshal.Marshalizer,
) *nftsProcessor {
	return &nftsProcessor{
		pubKeyConverter: pubKeyConverter,
		marshalizer:     marshalizer,
		nftOperationsIdentifiers: map[string]struct{}{
			core.BuiltInFunctionDCDTNFTBurn:   {},
			core.BuiltInFunctionDCDTNFTCreate: {},
			core.BuiltInFunctionDCDTWipe:      {},
		},
	}
}

func (np *nftsProcessor) processEvent(args *argsProcessEvent) argOutputProcessEvent {
	eventIdentifier := string(args.event.GetIdentifier())
	_, ok := np.nftOperationsIdentifiers[eventIdentifier]
	if !ok {
		return argOutputProcessEvent{}
	}

	// topics contains:
	// [0] --> token identifier
	// [1] --> nonce of the NFT (bytes)
	// [2] --> value
	// [3] --> receiver NFT address in case of NFTTransfer
	//     --> DCDT token data in case of NFTCreate
	topics := args.event.GetTopics()
	nonceBig := big.NewInt(0).SetBytes(topics[1])
	if nonceBig.Uint64() == 0 {
		// this is a fungible token so we should return
		return argOutputProcessEvent{}
	}

	sender := args.event.GetAddress()
	senderShardID := sharding.ComputeShardID(sender, args.numOfShards)
	if senderShardID == args.selfShardID {
		np.processNFTEventOnSender(args.event, args.tokens, args.tokensSupply, args.timestamp)
	}

	token := string(topics[0])
	identifier := converters.ComputeTokenIdentifier(token, nonceBig.Uint64())

	if !np.shouldAddReceiverData(args) {
		return argOutputProcessEvent{
			processed: true,
		}
	}

	receiver := args.event.GetTopics()[3]
	receiverShardID := sharding.ComputeShardID(receiver, args.numOfShards)
	if receiverShardID != args.selfShardID {
		return argOutputProcessEvent{
			processed: true,
		}
	}

	if eventIdentifier == core.BuiltInFunctionDCDTWipe {
		args.tokensSupply.Add(&data.TokenInfo{
			Token:      token,
			Identifier: identifier,
			Timestamp:  time.Duration(args.timestamp),
			Nonce:      nonceBig.Uint64(),
		})
	}

	return argOutputProcessEvent{
		processed: true,
	}
}

func (np *nftsProcessor) shouldAddReceiverData(args *argsProcessEvent) bool {
	eventIdentifier := string(args.event.GetIdentifier())
	isWrongIdentifier := eventIdentifier != core.BuiltInFunctionDCDTNFTTransfer &&
		eventIdentifier != core.BuiltInFunctionMultiDCDTNFTTransfer && eventIdentifier != core.BuiltInFunctionDCDTWipe

	if isWrongIdentifier || len(args.event.GetTopics()) < numTopicsWithReceiverAddress {
		return false
	}

	return true
}

func (np *nftsProcessor) processNFTEventOnSender(
	event coreData.EventHandler,
	tokensCreateInfo data.TokensHandler,
	tokensSupply data.TokensHandler,
	timestamp uint64,
) {
	topics := event.GetTopics()
	token := string(topics[0])
	nonceBig := big.NewInt(0).SetBytes(topics[1])
	eventIdentifier := string(event.GetIdentifier())
	if eventIdentifier == core.BuiltInFunctionDCDTNFTBurn || eventIdentifier == core.BuiltInFunctionDCDTWipe {
		tokensSupply.Add(&data.TokenInfo{
			Token:      token,
			Identifier: converters.ComputeTokenIdentifier(token, nonceBig.Uint64()),
			Timestamp:  time.Duration(timestamp),
			Nonce:      nonceBig.Uint64(),
		})
	}

	isNFTCreate := eventIdentifier == core.BuiltInFunctionDCDTNFTCreate
	shouldReturn := !isNFTCreate || len(topics) < numTopicsWithReceiverAddress
	if shouldReturn {
		return
	}

	dcdtTokenBytes := topics[3]
	dcdtToken := &dcdt.DCDigitalToken{}
	err := np.marshalizer.Unmarshal(dcdtToken, dcdtTokenBytes)
	if err != nil {
		log.Warn("nftsProcessor.processNFTEventOnSender() cannot urmarshal", "error", err.Error())
		return
	}

	tokenMetaData := converters.PrepareTokenMetaData(np.convertMetaData(dcdtToken.TokenMetaData))
	tokensCreateInfo.Add(&data.TokenInfo{
		Token:      token,
		Identifier: converters.ComputeTokenIdentifier(token, nonceBig.Uint64()),
		Timestamp:  time.Duration(timestamp),
		Data:       tokenMetaData,
		Nonce:      nonceBig.Uint64(),
	})
}

func (np *nftsProcessor) convertMetaData(metaData *dcdt.MetaData) *alteredAccount.TokenMetaData {
	if metaData == nil {
		return nil
	}
	encodedCreatorAddr, err := np.pubKeyConverter.Encode(metaData.Creator)
	if err != nil {
		log.Warn("nftsProcessor.convertMetaData", "cannot encode creator address", "error", err, "address", metaData.Creator)
	}

	return &alteredAccount.TokenMetaData{
		Nonce:      metaData.Nonce,
		Name:       string(metaData.Name),
		Creator:    encodedCreatorAddr,
		Royalties:  metaData.Royalties,
		Hash:       metaData.Hash,
		URIs:       metaData.URIs,
		Attributes: metaData.Attributes,
	}
}
