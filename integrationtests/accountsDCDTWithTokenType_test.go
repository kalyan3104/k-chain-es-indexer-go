//go:build integrationtests

package integrationtests

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/kalyan3104/k-chain-core-go/core"
	"github.com/kalyan3104/k-chain-core-go/data/alteredAccount"
	dataBlock "github.com/kalyan3104/k-chain-core-go/data/block"
	"github.com/kalyan3104/k-chain-core-go/data/dcdt"
	"github.com/kalyan3104/k-chain-core-go/data/outport"
	"github.com/kalyan3104/k-chain-core-go/data/transaction"
	indexerdata "github.com/kalyan3104/k-chain-es-indexer-go/process/dataindexer"
	"github.com/stretchr/testify/require"
)

func TestIndexAccountDCDTWithTokenType(t *testing.T) {
	setLogLevelDebug()

	esClient, err := createESClient(esURL)
	require.Nil(t, err)

	// ################ ISSUE NON FUNGIBLE TOKEN ##########################
	esProc, err := CreateElasticProcessor(esClient)
	require.Nil(t, err)

	body := &dataBlock.Body{}
	header := &dataBlock.Header{
		Round:     50,
		ShardID:   core.MetachainShardId,
		TimeStamp: 5040,
	}

	address := "moa1sqy2ywvswp09ef7qwjhv8zwr9kzz3xas6y2ye5nuryaz0wcnfzzs7cfj8p"
	pool := &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Address: decodeAddress(address),
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address),
							Identifier: []byte("issueSemiFungible"),
							Topics:     [][]byte{[]byte("SEMI-abcd"), []byte("SEMI-token"), []byte("SEM"), []byte(core.SemiFungibleDCDT)},
						},
						nil,
					},
				},
			},
		},
	}

	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, map[string]*alteredAccount.AlteredAccount{}, testNumOfShards))
	require.Nil(t, err)

	ids := []string{"SEMI-abcd"}
	genericResponse := &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/accountsDCDTWithTokenType/token-after-issue.json"), string(genericResponse.Docs[0].Source))

	// ################ CREATE SEMI FUNGIBLE TOKEN ##########################
	coreAlteredAccounts := map[string]*alteredAccount.AlteredAccount{
		address: {
			Address: address,
			Balance: "1000",
			Tokens: []*alteredAccount.AccountTokenData{
				{
					Identifier: "SEMI-abcd",
					Balance:    "1000",
					Nonce:      2,
					Properties: "3032",
					MetaData: &alteredAccount.TokenMetaData{
						Creator: "creator",
					},
				},
			},
		},
	}
	esProc, err = CreateElasticProcessor(esClient)
	require.Nil(t, err)

	header = &dataBlock.Header{
		Round:     51,
		TimeStamp: 5600,
		ShardID:   2,
	}

	dcdtData := &dcdt.DCDigitalToken{
		TokenMetaData: &dcdt.MetaData{
			Creator: []byte("creator"),
		},
	}
	dcdtDataBytes, _ := json.Marshal(dcdtData)

	pool = &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Address: decodeAddress(address),
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address),
							Identifier: []byte(core.BuiltInFunctionDCDTNFTCreate),
							Topics:     [][]byte{[]byte("SEMI-abcd"), big.NewInt(2).Bytes(), big.NewInt(1).Bytes(), dcdtDataBytes},
						},
						nil,
					},
				},
			},
		},
	}

	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, coreAlteredAccounts, testNumOfShards))
	require.Nil(t, err)

	ids = []string{fmt.Sprintf("%s-SEMI-abcd-02", address)}
	genericResponse = &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.AccountsDCDTIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/accountsDCDTWithTokenType/account-dcdt.json"), string(genericResponse.Docs[0].Source))

}

func TestIndexAccountDCDTWithTokenTypeShardFirstAndMetachainAfter(t *testing.T) {
	setLogLevelDebug()

	esClient, err := createESClient(esURL)
	require.Nil(t, err)

	// ################ CREATE SEMI FUNGIBLE TOKEN #########################
	body := &dataBlock.Body{}

	address := "moa1l29zsl2dqq988kvr2y0xlfv9ydgnvhzkatfd8ccalpag265pje8q7lelre"
	coreAlteredAccounts := map[string]*alteredAccount.AlteredAccount{
		address: {
			Address: address,
			Balance: "1000",
			Tokens: []*alteredAccount.AccountTokenData{
				{
					Identifier: "TTTT-abcd",
					Nonce:      2,
					Balance:    "1000",
					Properties: "3032",
					MetaData: &alteredAccount.TokenMetaData{
						Creator: "moa1l29zsl2dqq988kvr2y0xlfv9ydgnvhzkatfd8ccalpag265pje8q7lelre",
					},
				},
			},
		},
	}
	esProc, err := CreateElasticProcessor(esClient)
	require.Nil(t, err)

	header := &dataBlock.Header{
		Round:     51,
		TimeStamp: 5600,
		ShardID:   2,
	}

	dcdtData := &dcdt.DCDigitalToken{
		TokenMetaData: &dcdt.MetaData{
			Creator: decodeAddress(address),
		},
	}
	dcdtDataBytes, _ := json.Marshal(dcdtData)

	pool := &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Address: decodeAddress(address),
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address),
							Identifier: []byte(core.BuiltInFunctionDCDTNFTCreate),
							Topics:     [][]byte{[]byte("TTTT-abcd"), big.NewInt(2).Bytes(), big.NewInt(1).Bytes(), dcdtDataBytes},
						},
						nil,
					},
				},
			},
		},
	}

	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, coreAlteredAccounts, testNumOfShards))
	require.Nil(t, err)

	ids := []string{fmt.Sprintf("%s-TTTT-abcd-02", address)}
	genericResponse := &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.AccountsDCDTIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/accountsDCDTWithTokenType/account-dcdt-without-type.json"), string(genericResponse.Docs[0].Source))

	time.Sleep(time.Second)

	// ################ ISSUE NON FUNGIBLE TOKEN ##########################
	header = &dataBlock.Header{
		Round:     50,
		TimeStamp: 5040,
		ShardID:   core.MetachainShardId,
	}

	esProc, err = CreateElasticProcessor(esClient)
	require.Nil(t, err)

	pool = &outport.TransactionPool{
		Logs: []*outport.LogData{
			{
				TxHash: hex.EncodeToString([]byte("h1")),
				Log: &transaction.Log{
					Address: decodeAddress(address),
					Events: []*transaction.Event{
						{
							Address:    decodeAddress(address),
							Identifier: []byte("issueSemiFungible"),
							Topics:     [][]byte{[]byte("TTTT-abcd"), []byte("TTTT-token"), []byte("SEM"), []byte(core.SemiFungibleDCDT)},
						},
						nil,
					},
				},
			},
		},
	}

	err = esProc.SaveTransactions(createOutportBlockWithHeader(body, header, pool, map[string]*alteredAccount.AlteredAccount{}, testNumOfShards))
	require.Nil(t, err)

	ids = []string{"TTTT-abcd"}
	genericResponse = &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/accountsDCDTWithTokenType/semi-fungible-token.json"), string(genericResponse.Docs[0].Source))

	ids = []string{fmt.Sprintf("%s-TTTT-abcd-02", address)}
	genericResponse = &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.AccountsDCDTIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/accountsDCDTWithTokenType/account-dcdt-with-type.json"), string(genericResponse.Docs[0].Source))

	ids = []string{"TTTT-abcd-02"}
	genericResponse = &GenericResponse{}
	err = esClient.DoMultiGet(context.Background(), ids, indexerdata.TokensIndex, true, genericResponse)
	require.Nil(t, err)
	require.JSONEq(t, readExpectedResult("./testdata/accountsDCDTWithTokenType/semi-fungible-token-after-create.json"), string(genericResponse.Docs[0].Source))
}
