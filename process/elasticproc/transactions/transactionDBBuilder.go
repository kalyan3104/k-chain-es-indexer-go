package transactions

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/kalyan3104/k-chain-core-go/core"
	"github.com/kalyan3104/k-chain-core-go/core/sharding"
	coreData "github.com/kalyan3104/k-chain-core-go/data"
	"github.com/kalyan3104/k-chain-core-go/data/block"
	"github.com/kalyan3104/k-chain-core-go/data/outport"
	"github.com/kalyan3104/k-chain-core-go/data/receipt"
	"github.com/kalyan3104/k-chain-core-go/data/rewardTx"
	"github.com/kalyan3104/k-chain-es-indexer-go/data"
	"github.com/kalyan3104/k-chain-es-indexer-go/process/dataindexer"
	"github.com/kalyan3104/k-chain-es-indexer-go/process/elasticproc/converters"
)

type dbTransactionBuilder struct {
	addressPubkeyConverter core.PubkeyConverter
	dataFieldParser        DataFieldParser
	balanceConverter       dataindexer.BalanceConverter
}

func newTransactionDBBuilder(
	addressPubkeyConverter core.PubkeyConverter,
	dataFieldParser DataFieldParser,
	balanceConverter dataindexer.BalanceConverter,
) *dbTransactionBuilder {
	return &dbTransactionBuilder{
		addressPubkeyConverter: addressPubkeyConverter,
		dataFieldParser:        dataFieldParser,
		balanceConverter:       balanceConverter,
	}
}

func (dtb *dbTransactionBuilder) prepareTransaction(
	txInfo *outport.TxInfo,
	txHash []byte,
	mbHash []byte,
	mb *block.MiniBlock,
	header coreData.HeaderHandler,
	txStatus string,
	numOfShards uint32,
) *data.Transaction {
	tx := txInfo.Transaction

	isScCall := core.IsSmartContractAddress(tx.RcvAddr)
	res := dtb.dataFieldParser.Parse(tx.Data, tx.SndAddr, tx.RcvAddr, numOfShards)

	receiverAddr := dtb.addressPubkeyConverter.SilentEncode(tx.RcvAddr, log)
	senderAddr := dtb.addressPubkeyConverter.SilentEncode(tx.SndAddr, log)
	receiversAddr, _ := dtb.addressPubkeyConverter.EncodeSlice(res.Receivers)

	receiverShardID := mb.ReceiverShardID
	if mb.Type == block.InvalidBlock {
		receiverShardID = sharding.ComputeShardID(tx.RcvAddr, numOfShards)
	}

	valueNum, err := dtb.balanceConverter.ConvertBigValueToFloat(tx.Value)
	if err != nil {
		log.Warn("dbTransactionBuilder.prepareTransaction: cannot compute value as num", "value", tx.Value,
			"hash", txHash, "error", err)
	}

	feeInfo := getFeeInfo(txInfo)
	feeNum, err := dtb.balanceConverter.ConvertBigValueToFloat(feeInfo.Fee)
	if err != nil {
		log.Warn("dbTransactionBuilder.prepareTransaction: cannot compute transaction fee as num", "fee", feeInfo.Fee,
			"hash", txHash, "error", err)
	}
	dcdtValuesNum, err := dtb.balanceConverter.ComputeSliceOfStringsAsFloat(res.DCDTValues)
	if err != nil {
		log.Warn("dbTransactionBuilder.prepareTransaction: cannot compute dcdt values as num",
			"dcdt values", res.DCDTValues, "hash", txHash, "error", err)
	}

	var dcdtValues []string
	if areDCDTValuesOK(res.DCDTValues) {
		dcdtValues = res.DCDTValues
	}
	guardianAddress := ""
	if len(tx.GuardianAddr) > 0 {
		guardianAddress = dtb.addressPubkeyConverter.SilentEncode(tx.GuardianAddr, log)
	}

	senderUserName := converters.TruncateFieldIfExceedsMaxLengthBase64(string(tx.SndUserName))
	receiverUserName := converters.TruncateFieldIfExceedsMaxLengthBase64(string(tx.RcvUserName))
	return &data.Transaction{
		Hash:              hex.EncodeToString(txHash),
		MBHash:            hex.EncodeToString(mbHash),
		Nonce:             tx.Nonce,
		Round:             header.GetRound(),
		Value:             tx.Value.String(),
		Receiver:          receiverAddr,
		Sender:            senderAddr,
		ValueNum:          valueNum,
		ReceiverShard:     receiverShardID,
		SenderShard:       mb.SenderShardID,
		GasPrice:          tx.GasPrice,
		GasLimit:          tx.GasLimit,
		Data:              tx.Data,
		Signature:         hex.EncodeToString(tx.Signature),
		Timestamp:         time.Duration(header.GetTimeStamp()),
		Status:            txStatus,
		GasUsed:           feeInfo.GasUsed,
		InitialPaidFee:    feeInfo.InitialPaidFee.String(),
		Fee:               feeInfo.Fee.String(),
		FeeNum:            feeNum,
		ReceiverUserName:  []byte(receiverUserName),
		SenderUserName:    []byte(senderUserName),
		IsScCall:          isScCall,
		Operation:         res.Operation,
		Function:          converters.TruncateFieldIfExceedsMaxLength(res.Function),
		DCDTValues:        dcdtValues,
		DCDTValuesNum:     dcdtValuesNum,
		Tokens:            converters.TruncateSliceElementsIfExceedsMaxLength(res.Tokens),
		Receivers:         receiversAddr,
		ReceiversShardIDs: res.ReceiversShardID,
		IsRelayed:         res.IsRelayed,
		Version:           tx.Version,
		GuardianAddress:   guardianAddress,
		GuardianSignature: hex.EncodeToString(tx.GuardianSignature),
	}
}

func (dtb *dbTransactionBuilder) prepareRewardTransaction(
	rTx *rewardTx.RewardTx,
	txHash []byte,
	mbHash []byte,
	mb *block.MiniBlock,
	header coreData.HeaderHandler,
	txStatus string,
) *data.Transaction {
	valueNum, err := dtb.balanceConverter.ConvertBigValueToFloat(rTx.Value)
	if err != nil {
		log.Warn("dbTransactionBuilder.prepareRewardTransaction cannot compute value as num", "value", rTx.Value,
			"hash", txHash, "error", err)
	}

	receiverAddr := dtb.addressPubkeyConverter.SilentEncode(rTx.RcvAddr, log)

	return &data.Transaction{
		Hash:          hex.EncodeToString(txHash),
		MBHash:        hex.EncodeToString(mbHash),
		Nonce:         0,
		Round:         rTx.Round,
		Value:         rTx.Value.String(),
		ValueNum:      valueNum,
		Receiver:      receiverAddr,
		Sender:        fmt.Sprintf("%d", core.MetachainShardId),
		ReceiverShard: mb.ReceiverShardID,
		SenderShard:   mb.SenderShardID,
		GasPrice:      0,
		GasLimit:      0,
		Data:          make([]byte, 0),
		Signature:     "",
		Timestamp:     time.Duration(header.GetTimeStamp()),
		Status:        txStatus,
		Operation:     rewardsOperation,
	}
}

func (dtb *dbTransactionBuilder) prepareReceipt(
	recHashHex string,
	rec *receipt.Receipt,
	header coreData.HeaderHandler,
) *data.Receipt {
	senderAddr := dtb.addressPubkeyConverter.SilentEncode(rec.SndAddr, log)

	return &data.Receipt{
		Hash:      recHashHex,
		Value:     rec.Value.String(),
		Sender:    senderAddr,
		Data:      string(rec.Data),
		TxHash:    hex.EncodeToString(rec.TxHash),
		Timestamp: time.Duration(header.GetTimeStamp()),
	}
}
