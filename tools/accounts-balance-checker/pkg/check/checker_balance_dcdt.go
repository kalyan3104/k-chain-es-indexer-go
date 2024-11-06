package check

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kalyan3104/k-chain-core-go/core"
	"github.com/kalyan3104/k-chain-es-indexer-go/tools/accounts-balance-checker/pkg/utils"
)

const (
	maxDocumentsFromES = 9999
	accountsdcdtIndex  = "accountsdcdt"
	operationsIndex    = "operations"

	allTokensEndpoint    = "/address/%s/dcdt"
	specificDCDTEndpoint = allTokensEndpoint + "/%s"
	specificNFTEndpoint  = "/address/%s/nft/%s/nonce/%d"
)

var countTotalCompared uint64 = 0

// CheckDCDTBalances will compare all the DCDT balances from the Elasticsearch with the results from gateway
func (bc *balanceChecker) CheckDCDTBalances() error {
	balancesFromEs, err := bc.getAccountsByQuery(matchAllQuery)
	if err != nil {
		return err
	}

	log.Info("total accounts with DCDT tokens ", "count", len(balancesFromEs))

	maxGoroutines := bc.maxNumberOfParallelRequests
	done, wg := make(chan struct{}, maxGoroutines), &sync.WaitGroup{}
	for addr, tokenBalanceMap := range balancesFromEs {
		done <- struct{}{}
		wg.Add(1)

		atomic.AddUint64(&countTotalCompared, 1)
		go bc.compareBalancesFromES(addr, tokenBalanceMap, done, wg)
	}

	wg.Wait()

	log.Info("done", "total compared", countTotalCompared)

	return nil
}

func (bc *balanceChecker) compareBalancesFromES(addr string, tokenBalanceMap map[string]string, done chan struct{}, wg *sync.WaitGroup) {
	defer func() {
		<-done
		wg.Done()
	}()

	decoded, errD := bc.pubKeyConverter.Decode(addr)
	if errD != nil {
		log.Warn("cannot decode address", "address", addr, "error", errD)
		return
	}

	if core.IsSmartContractAddress(decoded) {
		bc.checkBalancesSC(addr, tokenBalanceMap)
		return
	}

	balancesFromProxy, errP := bc.getBalancesFromProxy(addr)
	if errP != nil {
		log.Warn("cannot get balances from proxy", "address", addr, "error", errP)
	}

	tryAgain := bc.compareBalances(tokenBalanceMap, balancesFromProxy, addr, true)
	if tryAgain {
		err := bc.getFromESAndCompare(addr, balancesFromProxy, len(tokenBalanceMap))
		if err != nil {
			log.Warn("cannot compare second time", "address", addr, "error", err)
		}
		return
	}
}

func (bc *balanceChecker) getFromESAndCompare(address string, balancesFromProxy map[string]string, numBalancesFromEs int) error {
	log.Info("second compare", "address", address, "total compared till now", atomic.LoadUint64(&countTotalCompared))

	balancesES, err := bc.getDCDTBalancesFromES(address, numBalancesFromEs)
	if err != nil {
		return err
	}

	_ = bc.compareBalances(balancesES.getBalancesForAddress(address), balancesFromProxy, address, false)

	return nil
}

func (bc *balanceChecker) getDCDTBalancesFromES(address string, numOfBalances int) (balancesDCDT, error) {
	encoded, _ := encodeQuery(getBalancesByAddress(address))

	if numOfBalances > maxDocumentsFromES {
		log.Info("bc.getDCDTBalancesFromES", "number of balances", numOfBalances, "address", address)
		return bc.getAccountsByQuery(encoded.String())
	}

	accountsResponse := &ResponseAccounts{}
	err := bc.esClient.DoGetRequest(&encoded, accountsdcdtIndex, accountsResponse, maxDocumentsFromES)
	if err != nil {
		return nil, err
	}

	balancesES := newBalancesDCDT()
	balancesES.extractBalancesFromResponse(accountsResponse)

	return balancesES, nil
}

func (bc *balanceChecker) compareBalances(balancesFromES, balancesFromProxy map[string]string, address string, firstCompare bool) (tryAgain bool) {
	copyBalancesProxy := make(map[string]string)
	for k, v := range balancesFromProxy {
		copyBalancesProxy[k] = v
	}

	for tokenIdentifier, balanceES := range balancesFromES {
		balanceProxy, ok := copyBalancesProxy[tokenIdentifier]
		if !ok && firstCompare {
			return true
		}

		if !ok {
			timestampLast, id := bc.getLasTimeWhenBalanceWasChanged(tokenIdentifier, address)
			timestampString := formatTimestamp(int64(timestampLast))

			log.Warn("extra balance in ES", "address", address,
				"token identifier", tokenIdentifier,
				"data", timestampString,
				"id", id)

			err := bc.deleteExtraBalance(address, tokenIdentifier, uint64(timestampLast), accountsdcdtIndex)
			if err != nil {
				log.Warn("cannot remove balance from es",
					"addr", address, "identifier", tokenIdentifier, "error", err)
			}

			continue
		}

		delete(copyBalancesProxy, tokenIdentifier)

		if balanceES != balanceProxy && firstCompare {
			return true
		}

		if balanceES != balanceProxy {
			timestampLast, id := bc.getLasTimeWhenBalanceWasChanged(tokenIdentifier, address)
			timestampString := formatTimestamp(int64(timestampLast))

			err := bc.fixWrongBalance(address, tokenIdentifier, uint64(timestampLast), balanceProxy, accountsdcdtIndex)
			if err != nil {
				log.Warn("cannot update balance from es", "addr", address, "identifier", tokenIdentifier)
			}

			log.Warn("different balance", "address", address,
				"token identifier", tokenIdentifier,
				"balance from ES", balanceES,
				"balance from proxy", balanceProxy,
				"data", timestampString,
				"id", id,
			)
			continue
		}
	}

	if len(copyBalancesProxy) > 0 && firstCompare {
		return true
	}

	for tokenIdentifier, balance := range copyBalancesProxy {
		if balance == "0" {
			// this if for in case of token was frozen and after that wipe
			continue
		}

		log.Warn("missing balance from ES", "address", address,
			"token identifier", tokenIdentifier, "balance", balance,
		)
	}

	return false
}

func (bc *balanceChecker) getLasTimeWhenBalanceWasChanged(identifier, address string) (time.Duration, string) {
	query := queryGetLastTxForToken(identifier, address)
	if identifier == "" {
		query = queryGetLastOperationForAddress(address)
	}

	txResponse := &ResponseTransactions{}
	err := bc.esClient.DoGetRequest(query, operationsIndex, txResponse, 1)
	if err != nil {
		log.Warn("bc.getLasTimeWhenBalanceWasChanged", "identifier", identifier, "addr", address, "error", err)
		return 0, ""
	}

	if len(txResponse.Hits.Hits) == 0 {
		return 0, ""
	}

	return txResponse.Hits.Hits[0].Source.Timestamp, txResponse.Hits.Hits[0].ID
}

func (bc *balanceChecker) getBalancesFromProxy(address string) (map[string]string, error) {
	responseBalancesProxy := &BalancesDCDTResponse{}
	err := bc.restClient.CallGetRestEndPoint(fmt.Sprintf(allTokensEndpoint, address), responseBalancesProxy)
	if err != nil {
		return nil, err
	}
	if responseBalancesProxy.Error != "" {
		return nil, errors.New(responseBalancesProxy.Error)
	}

	balances := make(map[string]string)
	for tokenIdentifier, tokenData := range responseBalancesProxy.Data.DCDTS {
		balances[tokenIdentifier] = tokenData.Balance
	}

	return balances, nil
}

func (bc *balanceChecker) getAccountsByQuery(query string) (balancesDCDT, error) {
	defer utils.LogExecutionTime(log, time.Now(), "get all accounts with DCDT tokens from ES")

	balances := newBalancesDCDT()

	countAccountsDCDT := 0
	handlerFunc := func(responseBytes []byte) error {
		accountsRes := &ResponseAccounts{}
		err := json.Unmarshal(responseBytes, accountsRes)
		if err != nil {
			return err
		}

		balances.extractBalancesFromResponse(accountsRes)

		countAccountsDCDT++
		log.Info("read accounts balance from es", "count", countAccountsDCDT)

		return nil
	}

	err := bc.esClient.DoScrollRequestAllDocuments(
		accountsdcdtIndex,
		[]byte(query),
		handlerFunc,
	)
	if err != nil {
		return nil, err
	}

	return balances, nil
}

func (bc *balanceChecker) handlerFuncScrollAccountDCDT(responseBytes []byte) error {
	accountsRes := &ResponseAccounts{}
	err := json.Unmarshal(responseBytes, accountsRes)
	if err != nil {
		return err
	}

	return nil
}

func formatTimestamp(timestamp int64) string {
	if timestamp == 0 {
		return "0"
	}

	tm := time.Unix(timestamp, 0)

	return tm.Format("2006-01-02-15:04:05")
}
