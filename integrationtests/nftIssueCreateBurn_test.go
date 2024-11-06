//go:build integrationtests

package integrationtests

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/kalyan3104/k-chain-core-go/core"
	dataBlock "github.com/kalyan3104/k-chain-core-go/data/block"
	"github.com/kalyan3104/k-chain-core-go/data/dcdt"
	"github.com/kalyan3104/k-chain-core-go/data/outport"
	"github.com/kalyan3104/k-chain-core-go/data/transaction"
	indexerdata "github.com/kalyan3104/k-chain-es-indexer-go/process/dataindexer"
	"github.com/stretchr/testify/require"
)

func TestIssueNFTCreateAndBurn(t *testing.T) {
	setLogLevelDebug()

	esClient, err := createESClient(esURL)
	require.Nil(t, err)

	// ################ ISSUE NON FUNGIBLE TOKEN #########################

	esProc, err := CreateElasticProcessor(esClient)
	require.Nil(t, err)

	body := &dataBlock.Body{}
	header := &dataBlock.Header{
		Round:     50,
		TimeStamp: 5040,
		ShardID:   core.MetachainShardId,
	}

	address1 := "moa1ju8pkvg57cwdmjsjx58jlmnuf4l9yspstrhr9tgsrt98n9edpm2qx8wte4"
	pool := &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Address: decodeAddress(address1),
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address1),
							Identifier: []byte("issueNonFungible"),
							Topics:     [][]byte{[]byte("NON-abcd"), []byte("NON-token"), []byte("NON"), []byte(core.NonFungibleDCDT)},
						},
						nil,
					},
				},
			},
		},
	}

	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, nil, testNumOfShards))
	require.Nil(t, err)

	ids := []string{"NON-abcd"}
	genericResponse := &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/nftIssueCreateBurn/non-fungible-after-issue.json"), string(genericResponse.Docs[0].Source))

	// ################ CREATE NON FUNGIBLE TOKEN ##########################
	esProc, err = CreateElasticProcessor(esClient)
	require.Nil(t, err)

	header = &dataBlock.Header{
		Round:     51,
		TimeStamp: 5600,
		ShardID:   0,
	}

	dcdtData := &dcdt.DCDigitalToken{
		TokenMetaData: &dcdt.MetaData{
			Creator: decodeAddress(address1),
		},
	}
	dcdtDataBytes, _ := json.Marshal(dcdtData)

	pool = &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Address: decodeAddress(address1),
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address1),
							Identifier: []byte(core.BuiltInFunctionDCDTNFTCreate),
							Topics:     [][]byte{[]byte("NON-abcd"), big.NewInt(2).Bytes(), big.NewInt(1).Bytes(), dcdtDataBytes},
						},
						nil,
					},
				},
			},
		},
	}

	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, nil, testNumOfShards))
	require.Nil(t, err)

	ids = []string{"NON-abcd-02"}
	genericResponse = &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/nftIssueCreateBurn/non-fungible-after-create.json"), string(genericResponse.Docs[0].Source))

	// ################ BURN NON FUNGIBLE TOKEN ##########################

	header = &dataBlock.Header{
		Round:     52,
		TimeStamp: 5666,
		ShardID:   0,
	}

	pool = &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Address: decodeAddress(address1),
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address1),
							Identifier: []byte(core.BuiltInFunctionDCDTNFTBurn),
							Topics:     [][]byte{[]byte("NON-abcd"), big.NewInt(2).Bytes(), big.NewInt(1).Bytes(), decodeAddress(address1)},
						},
						nil,
					},
				},
			},
		},
	}

	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, nil, testNumOfShards))
	require.Nil(t, err)

	ids = []string{"NON-abcd-02"}
	genericResponse = &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.False(t, genericResponse.Docs[0].Found)
}
