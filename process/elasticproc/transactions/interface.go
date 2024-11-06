package transactions

import (
	"github.com/kalyan3104/k-chain-core-go/data/outport"
	datafield "github.com/kalyan3104/k-chain-vm-common-go/parsers/dataField"
)

// DataFieldParser defines what a data field parser should be able to do
type DataFieldParser interface {
	Parse(dataField []byte, sender, receiver []byte, numOfShards uint32) *datafield.ResponseParseData
}

type feeInfoHandler interface {
	GetFeeInfo() *outport.FeeInfo
}
