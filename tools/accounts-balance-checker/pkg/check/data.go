package check

import "github.com/kalyan3104/k-chain-es-indexer-go/data"

type ResponseTransactions struct {
	Hits struct {
		Hits []struct {
			ID     string           `json:"_id"`
			Source data.Transaction `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// BulkRequestResponse defines the structure of a bulk request response
type BulkRequestResponse struct {
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			Status int `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}

// ResponseAccounts holds the accounts response from Elasticsearch
type ResponseAccounts struct {
	Hits struct {
		Hits []struct {
			ID     string           `json:"_id"`
			Source data.AccountInfo `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// AccountResponse holds the account endpoint response
type AccountResponse struct {
	Data struct {
		Balance string `json:"balance"`
	} `json:"data"`
	Error string `json:"error"`
	Code  string `json:"code"`
}

// BalancesDCDTResponse holds the account dcdt balances endpoint response
type BalancesDCDTResponse struct {
	Data struct {
		DCDTS     map[string]*dcdtNFTTokenData `json:"dcdts"`
		TokenData *dcdtNFTTokenData            `json:"tokenData"`
	} `json:"data"`
	Error string `json:"error"`
	Code  string `json:"code"`
}

type dcdtNFTTokenData struct {
	TokenIdentifier string   `json:"tokenIdentifier"`
	Balance         string   `json:"balance"`
	Properties      string   `json:"properties,omitempty"`
	Name            string   `json:"name,omitempty"`
	Nonce           uint64   `json:"nonce,omitempty"`
	Creator         string   `json:"creator,omitempty"`
	Royalties       string   `json:"royalties,omitempty"`
	Hash            []byte   `json:"hash,omitempty"`
	URIs            [][]byte `json:"uris,omitempty"`
	Attributes      []byte   `json:"attributes,omitempty"`
}
