package accounts

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/kalyan3104/k-chain-core-go/core"
	"github.com/kalyan3104/k-chain-core-go/core/check"
	"github.com/kalyan3104/k-chain-core-go/data/alteredAccount"
	"github.com/kalyan3104/k-chain-es-indexer-go/data"
	"github.com/kalyan3104/k-chain-es-indexer-go/process/dataindexer"
	"github.com/kalyan3104/k-chain-es-indexer-go/process/elasticproc/converters"
	logger "github.com/kalyan3104/k-chain-logger-go"
)

var log = logger.GetOrCreate("indexer/process/accounts")

// accountsProcessor is a structure responsible for processing accounts
type accountsProcessor struct {
	addressPubkeyConverter core.PubkeyConverter
	balanceConverter       dataindexer.BalanceConverter
}

// NewAccountsProcessor will create a new instance of accounts processor
func NewAccountsProcessor(
	addressPubkeyConverter core.PubkeyConverter,
	balanceConverter dataindexer.BalanceConverter,
) (*accountsProcessor, error) {
	if check.IfNil(addressPubkeyConverter) {
		return nil, dataindexer.ErrNilPubkeyConverter
	}
	if check.IfNil(balanceConverter) {
		return nil, dataindexer.ErrNilBalanceConverter
	}

	return &accountsProcessor{
		addressPubkeyConverter: addressPubkeyConverter,
		balanceConverter:       balanceConverter,
	}, nil
}

// GetAccounts will get accounts for regular operations and dcdt operations
func (ap *accountsProcessor) GetAccounts(coreAlteredAccounts map[string]*alteredAccount.AlteredAccount) ([]*data.Account, []*data.AccountDCDT) {
	regularAccountsToIndex := make([]*data.Account, 0)
	accountsToIndexDCDT := make([]*data.AccountDCDT, 0)

	for _, alteredAccount := range coreAlteredAccounts {
		regularAccounts, dcdtAccounts := splitAlteredAccounts(alteredAccount)

		regularAccountsToIndex = append(regularAccountsToIndex, regularAccounts...)
		accountsToIndexDCDT = append(accountsToIndexDCDT, dcdtAccounts...)
	}

	return regularAccountsToIndex, accountsToIndexDCDT
}

func splitAlteredAccounts(
	account *alteredAccount.AlteredAccount,
) ([]*data.Account, []*data.AccountDCDT) {
	regularAccountsToIndex := make([]*data.Account, 0)
	accountsToIndexDCDT := make([]*data.AccountDCDT, 0)

	isSender, balanceChanged := false, false
	if account.AdditionalData != nil {
		isSender, balanceChanged = account.AdditionalData.IsSender, account.AdditionalData.BalanceChanged
	} else {
		log.Debug("accountsProcessor.splitAlteredAccounts - nil additional data")
	}

	//if the balance of the DCDT receiver is 0 the receiver is a new account most probably, and we should index it
	ignoreAddress := !balanceChanged && notZeroBalance(account.Balance) && !isSender
	if !ignoreAddress {
		regularAccountsToIndex = append(regularAccountsToIndex, &data.Account{
			UserAccount: account,
			IsSender:    isSender,
		})
	}

	for _, info := range account.Tokens {
		accountDCDT := &data.AccountDCDT{
			Account:         account,
			TokenIdentifier: info.Identifier,
			NFTNonce:        info.Nonce,
			IsSender:        isSender,
		}
		if info.AdditionalData != nil {
			accountDCDT.IsNFTCreate = info.AdditionalData.IsNFTCreate
		}

		accountsToIndexDCDT = append(accountsToIndexDCDT, accountDCDT)

	}

	return regularAccountsToIndex, accountsToIndexDCDT
}

func notZeroBalance(balance string) bool {
	return len(balance) > 0 && balance != "0"
}

// PrepareRegularAccountsMap will prepare a map of regular accounts
func (ap *accountsProcessor) PrepareRegularAccountsMap(timestamp uint64, accounts []*data.Account, shardID uint32) map[string]*data.AccountInfo {
	accountsMap := make(map[string]*data.AccountInfo)
	for _, userAccount := range accounts {
		address := userAccount.UserAccount.Address
		addressBytes, err := ap.addressPubkeyConverter.Decode(address)
		if err != nil {
			log.Warn("accountsProcessor.PrepareRegularAccountsMap: cannot decode address", "address", address, "error", err)
			continue
		}
		balance, ok := big.NewInt(0).SetString(userAccount.UserAccount.Balance, 10)
		if !ok {
			log.Warn("accountsProcessor.PrepareRegularAccountsMap: cannot cast account's balance to big int", "value", userAccount.UserAccount.Balance)
			continue
		}

		balanceAsFloat, err := ap.balanceConverter.ComputeBalanceAsFloat(balance)
		if err != nil {
			log.Warn("accountsProcessor.PrepareRegularAccountsMap: cannot compute balance as num",
				"balance", balance, "address", address, "error", err)
		}

		acc := &data.AccountInfo{
			Address:         address,
			Nonce:           userAccount.UserAccount.Nonce,
			Balance:         converters.BigIntToString(balance),
			BalanceNum:      balanceAsFloat,
			IsSender:        userAccount.IsSender,
			IsSmartContract: core.IsSmartContractAddress(addressBytes),
			Timestamp:       time.Duration(timestamp),
			ShardID:         shardID,
		}

		ap.addAdditionalDataInAccount(userAccount.UserAccount.AdditionalData, acc)

		accountsMap[address] = acc
	}

	return accountsMap
}

func (ap *accountsProcessor) addAdditionalDataInAccount(additionalData *alteredAccount.AdditionalAccountData, account *data.AccountInfo) {
	if additionalData == nil {
		return
	}

	account.UserName = additionalData.UserName
	account.CurrentOwner = additionalData.CurrentOwner
	account.RootHash = additionalData.RootHash
	account.CodeHash = additionalData.CodeHash
	account.CodeMetadata = additionalData.CodeMetadata

	ap.addDeveloperRewardsInAccount(additionalData, account)
}

func (ap *accountsProcessor) addDeveloperRewardsInAccount(additionalData *alteredAccount.AdditionalAccountData, account *data.AccountInfo) {
	if additionalData.DeveloperRewards == "" {
		return
	}

	developerRewardsBig, ok := big.NewInt(0).SetString(additionalData.DeveloperRewards, 10)
	if !ok {
		log.Warn("ap.addDeveloperRewardsInAccountInfo cannot convert developer rewards in number", "address", account.Address)
		return
	}

	account.DeveloperRewards = additionalData.DeveloperRewards

	developerRewardsNum, err := ap.balanceConverter.ComputeBalanceAsFloat(developerRewardsBig)
	if err != nil {
		log.Warn("accountsProcessor.addDeveloperRewardsInAccount: cannot compute developer rewards as num",
			"developer rewards", developerRewardsBig, "error", err)
	}

	account.DeveloperRewardsNum = developerRewardsNum
}

// PrepareAccountsMapDCDT will prepare a map of accounts with DCDT tokens
func (ap *accountsProcessor) PrepareAccountsMapDCDT(
	timestamp uint64,
	accounts []*data.AccountDCDT,
	tagsCount data.CountTags,
	shardID uint32,
) (map[string]*data.AccountInfo, data.TokensHandler) {
	tokensData := data.NewTokensInfo()
	accountsDCDTMap := make(map[string]*data.AccountInfo)
	for _, accountDCDT := range accounts {
		address := accountDCDT.Account.Address
		addressBytes, err := ap.addressPubkeyConverter.Decode(address)
		if err != nil {
			log.Warn("accountsProcessor.PrepareAccountsMapDCDT: cannot decode address", "address", address, "error", err)
			continue
		}
		balance, properties, tokenMetaData, err := ap.getDCDTInfo(accountDCDT)
		if err != nil {
			log.Warn("accountsProcessor.PrepareAccountsMapDCDT: cannot get dcdt info from account",
				"address", address,
				"error", err.Error())
			continue
		}

		if tokenMetaData != nil && accountDCDT.IsNFTCreate {
			tagsCount.ParseTags(tokenMetaData.Tags)
		}

		tokenIdentifier := converters.ComputeTokenIdentifier(accountDCDT.TokenIdentifier, accountDCDT.NFTNonce)
		balanceNum, err := ap.balanceConverter.ConvertBigValueToFloat(balance)
		if err != nil {
			log.Warn("accountsProcessor.PrepareAccountsMapDCDT: cannot compute dcdt balance as num",
				"balance", balance, "address", address, "error", err, "token", tokenIdentifier)
		}

		acc := &data.AccountInfo{
			Address:         address,
			TokenName:       accountDCDT.TokenIdentifier,
			TokenIdentifier: tokenIdentifier,
			TokenNonce:      accountDCDT.NFTNonce,
			Balance:         balance.String(),
			BalanceNum:      balanceNum,
			Properties:      properties,
			Frozen:          isFrozen(properties),
			IsSender:        accountDCDT.IsSender,
			IsSmartContract: core.IsSmartContractAddress(addressBytes),
			Data:            tokenMetaData,
			Timestamp:       time.Duration(timestamp),
			ShardID:         shardID,
		}

		if acc.TokenNonce == 0 {
			acc.Type = core.FungibleDCDT
		}

		keyInMap := fmt.Sprintf("%s-%s-%d", acc.Address, acc.TokenName, accountDCDT.NFTNonce)
		accountsDCDTMap[keyInMap] = acc

		if acc.Balance == "0" || acc.Balance == "" {
			continue
		}

		tokensData.Add(&data.TokenInfo{
			Token:      accountDCDT.TokenIdentifier,
			Identifier: tokenIdentifier,
		})
	}

	return accountsDCDTMap, tokensData
}

// PrepareAccountsHistory will prepare a map of accounts history balance from a map of accounts
func (ap *accountsProcessor) PrepareAccountsHistory(
	timestamp uint64,
	accounts map[string]*data.AccountInfo,
	shardID uint32,
) map[string]*data.AccountBalanceHistory {
	accountsMap := make(map[string]*data.AccountBalanceHistory)
	for _, userAccount := range accounts {
		acc := &data.AccountBalanceHistory{
			Address:         userAccount.Address,
			Balance:         userAccount.Balance,
			Timestamp:       time.Duration(timestamp),
			Token:           userAccount.TokenName,
			TokenNonce:      userAccount.TokenNonce,
			IsSender:        userAccount.IsSender,
			IsSmartContract: userAccount.IsSmartContract,
			Identifier:      converters.ComputeTokenIdentifier(userAccount.TokenName, userAccount.TokenNonce),
			ShardID:         shardID,
		}
		keyInMap := fmt.Sprintf("%s-%s-%d", acc.Address, acc.Token, acc.TokenNonce)
		accountsMap[keyInMap] = acc
	}

	return accountsMap
}

func (ap *accountsProcessor) getDCDTInfo(accountDCDT *data.AccountDCDT) (*big.Int, string, *data.TokenMetaData, error) {
	if accountDCDT.TokenIdentifier == "" {
		return big.NewInt(0), "", nil, nil
	}
	if accountDCDT.IsNFTOperation && accountDCDT.NFTNonce == 0 {
		return big.NewInt(0), "", nil, nil
	}

	accountTokenData := &alteredAccount.AccountTokenData{}
	for _, tokenData := range accountDCDT.Account.Tokens {
		if tokenData.Identifier == accountDCDT.TokenIdentifier && tokenData.Nonce == accountDCDT.NFTNonce {
			accountTokenData = tokenData
		}
	}

	value, _ := big.NewInt(0).SetString(accountTokenData.Balance, 10)
	if value == nil {
		return big.NewInt(0), "", nil, nil
	}

	tokenMetaData := converters.PrepareTokenMetaData(accountTokenData.MetaData)

	return value, accountTokenData.Properties, tokenMetaData, nil
}

// PutTokenMedataDataInTokens will put the TokenMedata in provided tokens data
func (ap *accountsProcessor) PutTokenMedataDataInTokens(tokensData []*data.TokenInfo, coreAlteredAccounts map[string]*alteredAccount.AlteredAccount) {
	for _, tokenData := range tokensData {
		if tokenData.Data != nil || tokenData.Nonce == 0 {
			continue
		}

		metadata, errLoad := ap.loadMetadataForToken(tokenData, coreAlteredAccounts)
		if errLoad != nil {
			log.Warn("accountsProcessor.PutTokenMedataDataInTokens: cannot load token metadata",
				"token identifier ", tokenData.Identifier,
				"error", errLoad.Error())

			continue
		}

		tokenData.Data = converters.PrepareTokenMetaData(metadata)
	}
}

func (ap *accountsProcessor) loadMetadataForToken(
	tokenData *data.TokenInfo,
	coreAlteredAccounts map[string]*alteredAccount.AlteredAccount,
) (*alteredAccount.TokenMetaData, error) {
	for _, account := range coreAlteredAccounts {
		for _, token := range account.Tokens {
			if tokenData.Token == token.Identifier && tokenData.Nonce == token.Nonce {
				return token.MetaData, nil
			}
		}
	}

	return nil, fmt.Errorf("%w for identifier %s and nonce %d", errTokenNotFound, tokenData.Identifier, tokenData.Nonce)
}

func isFrozen(properties string) bool {
	decoded, err := hex.DecodeString(properties)
	if err != nil {
		log.Debug("isFrozen() cannot decode token properties", "error", err)
		return false
	}
	if len(decoded) == 0 {
		return false
	}

	return (decoded[0] & 1) != 0
}
