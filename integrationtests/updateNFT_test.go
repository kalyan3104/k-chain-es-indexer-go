//go:build integrationtests

package integrationtests

import (
	"bytes"
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

func TestNFTUpdateMetadata(t *testing.T) {
	setLogLevelDebug()

	esClient, err := createESClient(esURL)
	require.Nil(t, err)

	bigUri := bytes.Repeat([]byte("a"), 50000)
	dcdtCreateData := &dcdt.DCDigitalToken{
		TokenMetaData: &dcdt.MetaData{
			URIs: [][]byte{[]byte("uri"), []byte("uri"), bigUri, bigUri, bigUri},
		},
	}
	marshalizedCreate, _ := json.Marshal(dcdtCreateData)

	esProc, err := CreateElasticProcessor(esClient)
	require.Nil(t, err)

	header := &dataBlock.Header{
		Round:     50,
		TimeStamp: 5040,
		ShardID:   1,
	}
	body := &dataBlock.Body{}

	// CREATE NFT data
	address := "moa1w7jyzuj6cv4ngw8luhlkakatjpmjh3ql95lmxphd3vssc4vpymkshwjytw"
	pool := &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address),
							Identifier: []byte(core.BuiltInFunctionDCDTNFTCreate),
							Topics:     [][]byte{[]byte("NFT-abcd"), big.NewInt(14).Bytes(), big.NewInt(1).Bytes(), marshalizedCreate},
						},
						nil,
					},
				},
			},
		},
	}
	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, nil, testNumOfShards))
	require.Nil(t, err)

	ids := []string{"NFT-abcd-0e"}
	genericResponse := &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/updateNFT/token.json"), string(genericResponse.Docs[0].Source))

	// Add URIS 1
	pool = &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address),
							Identifier: []byte(core.BuiltInFunctionDCDTNFTAddURI),
							Topics:     [][]byte{[]byte("NFT-abcd"), big.NewInt(14).Bytes(), big.NewInt(0).Bytes(), []byte("uri"), bigUri},
						},
						nil,
					},
				},
			},
		},
	}
	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, nil, testNumOfShards))
	require.Nil(t, err)

	// Add URIS 2 --- results should be the same
	pool = &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address),
							Identifier: []byte(core.BuiltInFunctionDCDTNFTAddURI),
							Topics:     [][]byte{[]byte("NFT-abcd"), big.NewInt(14).Bytes(), big.NewInt(0).Bytes(), []byte("uri"), bigUri},
						},
						nil,
					},
				},
			},
		},
	}
	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, nil, testNumOfShards))
	require.Nil(t, err)

	// Update attributes 1
	ids = []string{"NFT-abcd-0e"}
	genericResponse = &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/updateNFT/token-after-add-uris.json"), string(genericResponse.Docs[0].Source))

	pool = &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address),
							Identifier: []byte(core.BuiltInFunctionDCDTNFTUpdateAttributes),
							Topics:     [][]byte{[]byte("NFT-abcd"), big.NewInt(14).Bytes(), big.NewInt(0).Bytes(), []byte("tags:test,free,fun;description:This is a test description for an awesome nft;metadata:metadata-test")},
						},
						nil,
					},
				},
			},
		},
	}
	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, nil, testNumOfShards))
	require.Nil(t, err)

	ids = []string{"NFT-abcd-0e"}
	genericResponse = &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/updateNFT/token-after-update-attributes.json"), string(genericResponse.Docs[0].Source))

	// Update attributes 2

	pool = &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address),
							Identifier: []byte(core.BuiltInFunctionDCDTNFTUpdateAttributes),
							Topics:     [][]byte{[]byte("NFT-abcd"), big.NewInt(14).Bytes(), big.NewInt(0).Bytes(), []byte("something")},
						},
						nil,
					},
				},
			},
		},
	}
	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, nil, testNumOfShards))
	require.Nil(t, err)

	ids = []string{"NFT-abcd-0e"}
	genericResponse = &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/updateNFT/token-after-update-attributes-second.json"), string(genericResponse.Docs[0].Source))

	// Freeze nft
	pool = &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address),
							Identifier: []byte(core.BuiltInFunctionDCDTFreeze),
							Topics:     [][]byte{[]byte("NFT-abcd"), big.NewInt(14).Bytes(), big.NewInt(0).Bytes(), []byte("something")},
						},
						nil,
					},
				},
			},
		},
	}
	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, nil, testNumOfShards))
	require.Nil(t, err)
	ids = []string{"NFT-abcd-0e"}
	genericResponse = &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/updateNFT/token-after-freeze.json"), string(genericResponse.Docs[0].Source))

	// UnFreeze nft
	pool = &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address),
							Identifier: []byte(core.BuiltInFunctionDCDTUnFreeze),
							Topics:     [][]byte{[]byte("NFT-abcd"), big.NewInt(14).Bytes(), big.NewInt(0).Bytes(), []byte("something")},
						},
						nil,
					},
				},
			},
		},
	}
	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, nil, testNumOfShards))
	require.Nil(t, err)
	ids = []string{"NFT-abcd-0e"}
	genericResponse = &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/updateNFT/token-after-un-freeze.json"), string(genericResponse.Docs[0].Source))
}
