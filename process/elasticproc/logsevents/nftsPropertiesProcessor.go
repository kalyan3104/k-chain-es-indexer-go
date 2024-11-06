package logsevents

import (
	"math/big"

	"github.com/kalyan3104/k-chain-core-go/core"
	"github.com/kalyan3104/k-chain-es-indexer-go/data"
	"github.com/kalyan3104/k-chain-es-indexer-go/process/elasticproc/converters"
)

const minTopicsUpdate = 4

type nftsPropertiesProc struct {
	pubKeyConverter            core.PubkeyConverter
	propertiesChangeOperations map[string]struct{}
}

func newNFTsPropertiesProcessor(pubKeyConverter core.PubkeyConverter) *nftsPropertiesProc {
	return &nftsPropertiesProc{
		pubKeyConverter: pubKeyConverter,
		propertiesChangeOperations: map[string]struct{}{
			core.BuiltInFunctionDCDTNFTAddURI:           {},
			core.BuiltInFunctionDCDTNFTUpdateAttributes: {},
			core.BuiltInFunctionDCDTFreeze:              {},
			core.BuiltInFunctionDCDTUnFreeze:            {},
			core.BuiltInFunctionDCDTPause:               {},
			core.BuiltInFunctionDCDTUnPause:             {},
		},
	}
}

func (npp *nftsPropertiesProc) processEvent(args *argsProcessEvent) argOutputProcessEvent {
	//nolint
	eventIdentifier := string(args.event.GetIdentifier())
	_, ok := npp.propertiesChangeOperations[eventIdentifier]
	if !ok {
		return argOutputProcessEvent{}
	}

	callerAddress := npp.pubKeyConverter.SilentEncode(args.event.GetAddress(), log)
	if callerAddress == "" {
		return argOutputProcessEvent{
			processed: true,
		}
	}

	topics := args.event.GetTopics()
	if len(topics) == 1 {
		return npp.processPauseAndUnPauseEvent(eventIdentifier, string(topics[0]))
	}

	// topics contains:
	// [0] --> token identifier
	// [1] --> nonce of the NFT (bytes)
	// [2] --> value
	// [3:] --> modified data
	if len(topics) < minTopicsUpdate {
		return argOutputProcessEvent{
			processed: true,
		}
	}

	callerAddress = npp.pubKeyConverter.SilentEncode(args.event.GetAddress(), log)
	if callerAddress == "" {
		return argOutputProcessEvent{
			processed: true,
		}
	}

	nonceBig := big.NewInt(0).SetBytes(topics[1])
	if nonceBig.Uint64() == 0 {
		// this is a fungible token so we should return
		return argOutputProcessEvent{}
	}

	token := string(topics[0])
	identifier := converters.ComputeTokenIdentifier(token, nonceBig.Uint64())

	updateNFT := &data.NFTDataUpdate{
		Identifier: identifier,
		Address:    callerAddress,
	}

	switch eventIdentifier {
	case core.BuiltInFunctionDCDTNFTUpdateAttributes:
		updateNFT.NewAttributes = topics[3]
	case core.BuiltInFunctionDCDTNFTAddURI:
		updateNFT.URIsToAdd = topics[3:]
	case core.BuiltInFunctionDCDTFreeze:
		updateNFT.Freeze = true
	case core.BuiltInFunctionDCDTUnFreeze:
		updateNFT.UnFreeze = true
	}

	return argOutputProcessEvent{
		processed:     true,
		updatePropNFT: updateNFT,
	}
}

func (npp *nftsPropertiesProc) processPauseAndUnPauseEvent(eventIdentifier string, token string) argOutputProcessEvent {
	var updateNFT *data.NFTDataUpdate

	switch eventIdentifier {
	case core.BuiltInFunctionDCDTPause:
		updateNFT = &data.NFTDataUpdate{
			Identifier: token,
			Pause:      true,
		}
	case core.BuiltInFunctionDCDTUnPause:
		updateNFT = &data.NFTDataUpdate{
			Identifier: token,
			UnPause:    true,
		}
	}

	return argOutputProcessEvent{
		processed:     true,
		updatePropNFT: updateNFT,
	}
}
