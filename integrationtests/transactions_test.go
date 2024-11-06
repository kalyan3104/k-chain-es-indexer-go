//go:build integrationtests

package integrationtests

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"

	dataBlock "github.com/kalyan3104/k-chain-core-go/data/block"
	"github.com/kalyan3104/k-chain-core-go/data/outport"
	"github.com/kalyan3104/k-chain-core-go/data/transaction"
	indexerData "github.com/kalyan3104/k-chain-es-indexer-go/process/dataindexer"
	"github.com/stretchr/testify/require"
)

func TestElasticIndexerSaveTransactions(t *testing.T) {
	setLogLevelDebug()

	esClient, err := createESClient(esURL)
	require.Nil(t, err)

	esProc, err := CreateElasticProcessor(esClient)
	require.Nil(t, err)

	txHash := []byte("hash")
	header := &dataBlock.Header{
		Round:     50,
		TimeStamp: 5040,
	}
	body := &dataBlock.Body{
		MiniBlocks: dataBlock.MiniBlockSlice{
			{
				Type:            dataBlock.TxBlock,
				SenderShardID:   0,
				ReceiverShardID: 0,
				TxHashes:        [][]byte{txHash},
			},
		},
	}
	tx := &transaction.Transaction{
		Nonce:    1,
		SndAddr:  decodeAddress("moa1w7jyzuj6cv4ngw8luhlkakatjpmjh3ql95lmxphd3vssc4vpymkshwjytw"),
		RcvAddr:  decodeAddress("moa1ahmy0yjhjg87n755yv99nzla22zzwfud55sa69gk3anyxyyucq9q80wfj7"),
		GasLimit: 70000,
		GasPrice: 1000000000,
		Data:     []byte("transfer"),
		Value:    big.NewInt(1234),
	}

	txInfo := &outport.TxInfo{
		Transaction: tx,
		FeeInfo: &outport.FeeInfo{
			GasUsed:        62000,
			Fee:            big.NewInt(62000000000000),
			InitialPaidFee: big.NewInt(62080000000000),
		},
		ExecutionOrder: 0,
	}

	pool := &outport.TransactionPool{
		Transactions: map[string]*outport.TxInfo{
			hex.EncodeToString(txHash): txInfo,
		},
	}
	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, nil, testNumOfShards))
	require.Nil(t, err)

	ids := []string{hex.EncodeToString(txHash)}
	genericResponse := &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerData.TransactionsIndex, true, genericResponse)
	require.Nil(t, err)

	require.JSONEq(t,
		readExpectedResult("./testdata/transactions/move-balance.json"),
		string(genericResponse.Docs[0].Source),
	)
}
