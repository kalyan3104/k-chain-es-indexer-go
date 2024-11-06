package mock

import (
	"github.com/kalyan3104/k-chain-core-go/data/alteredAccount"
	"github.com/kalyan3104/k-chain-es-indexer-go/data"
)

// DBAccountsHandlerStub -
type DBAccountsHandlerStub struct {
	PrepareAccountsHistoryCalled   func(timestamp uint64, accounts map[string]*data.AccountInfo) map[string]*data.AccountBalanceHistory
	SerializeAccountsHistoryCalled func(accounts map[string]*data.AccountBalanceHistory, buffSlice *data.BufferSlice, index string) error
}

// GetAccounts -
func (dba *DBAccountsHandlerStub) GetAccounts(_ map[string]*alteredAccount.AlteredAccount) ([]*data.Account, []*data.AccountDCDT) {
	return nil, nil
}

// PrepareRegularAccountsMap -
func (dba *DBAccountsHandlerStub) PrepareRegularAccountsMap(_ uint64, _ []*data.Account, _ uint32) map[string]*data.AccountInfo {
	return nil
}

// PrepareAccountsMapDCDT -
func (dba *DBAccountsHandlerStub) PrepareAccountsMapDCDT(_ uint64, _ []*data.AccountDCDT, _ data.CountTags, _ uint32) (map[string]*data.AccountInfo, data.TokensHandler) {
	return nil, nil
}

// PrepareAccountsHistory -
func (dba *DBAccountsHandlerStub) PrepareAccountsHistory(timestamp uint64, accounts map[string]*data.AccountInfo, _ uint32) map[string]*data.AccountBalanceHistory {
	if dba.PrepareAccountsHistoryCalled != nil {
		return dba.PrepareAccountsHistoryCalled(timestamp, accounts)
	}

	return nil
}

// SerializeAccountsHistory -
func (dba *DBAccountsHandlerStub) SerializeAccountsHistory(accounts map[string]*data.AccountBalanceHistory, buffSlice *data.BufferSlice, index string) error {
	if dba.SerializeAccountsHistoryCalled != nil {
		return dba.SerializeAccountsHistoryCalled(accounts, buffSlice, index)
	}
	return nil
}

// SerializeAccounts -
func (dba *DBAccountsHandlerStub) SerializeAccounts(_ map[string]*data.AccountInfo, _ *data.BufferSlice, _ string) error {
	return nil
}

// SerializeAccountsDCDT -
func (dba *DBAccountsHandlerStub) SerializeAccountsDCDT(_ map[string]*data.AccountInfo, _ []*data.NFTDataUpdate, _ *data.BufferSlice, _ string) error {
	return nil
}

// SerializeNFTCreateInfo -
func (dba *DBAccountsHandlerStub) SerializeNFTCreateInfo(_ []*data.TokenInfo, _ *data.BufferSlice, _ string) error {
	return nil
}

// PutTokenMedataDataInTokens -
func (dba *DBAccountsHandlerStub) PutTokenMedataDataInTokens(_ []*data.TokenInfo, _ map[string]*alteredAccount.AlteredAccount) {
}

// SerializeTypeForProvidedIDs -
func (dba *DBAccountsHandlerStub) SerializeTypeForProvidedIDs(_ []string, _ string, _ *data.BufferSlice, _ string) error {
	return nil
}
