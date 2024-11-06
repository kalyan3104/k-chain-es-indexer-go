package logsevents

import (
	"unicode"

	"github.com/kalyan3104/k-chain-core-go/core"
	vmcommon "github.com/kalyan3104/k-chain-vm-common-go"
)

const (
	tokenTopicsIndex            = 0
	propertyPairStep            = 2
	dcdtPropertiesStartIndex    = 2
	minTopicsPropertiesAndRoles = 4
	upgradePropertiesEvent      = "upgradeProperties"
)

type dcdtPropertiesProc struct {
	pubKeyConverter            core.PubkeyConverter
	rolesOperationsIdentifiers map[string]struct{}
}

func newDcdtPropertiesProcessor(pubKeyConverter core.PubkeyConverter) *dcdtPropertiesProc {
	return &dcdtPropertiesProc{
		pubKeyConverter: pubKeyConverter,
		rolesOperationsIdentifiers: map[string]struct{}{
			core.BuiltInFunctionSetDCDTRole:                 {},
			core.BuiltInFunctionUnSetDCDTRole:               {},
			core.BuiltInFunctionDCDTNFTCreateRoleTransfer:   {},
			upgradePropertiesEvent:                          {},
			vmcommon.BuiltInFunctionDCDTUnSetBurnRoleForAll: {},
			vmcommon.BuiltInFunctionDCDTSetBurnRoleForAll:   {},
		},
	}
}

func (epp *dcdtPropertiesProc) processEvent(args *argsProcessEvent) argOutputProcessEvent {
	identifier := string(args.event.GetIdentifier())
	_, ok := epp.rolesOperationsIdentifiers[identifier]
	if !ok {
		return argOutputProcessEvent{}
	}

	topics := args.event.GetTopics()
	if len(topics) < minTopicsPropertiesAndRoles {
		return argOutputProcessEvent{
			processed: true,
		}
	}

	if identifier == upgradePropertiesEvent {
		return epp.extractTokenProperties(args)
	}

	if identifier == core.BuiltInFunctionDCDTNFTCreateRoleTransfer {
		return epp.extractDataNFTCreateRoleTransfer(args)
	}

	// topics contains:
	// [0] --> token identifier
	// [1] --> nonce of the NFT (bytes)
	// [2] --> value
	// [3:] --> roles to set or unset

	rolesBytes := topics[3:]
	ok = checkRolesBytes(rolesBytes)
	if !ok {
		return argOutputProcessEvent{
			processed: true,
		}
	}

	shouldAddRole := identifier == core.BuiltInFunctionSetDCDTRole || identifier == vmcommon.BuiltInFunctionDCDTSetBurnRoleForAll

	addrBech := epp.pubKeyConverter.SilentEncode(args.event.GetAddress(), log)
	for _, roleBytes := range rolesBytes {
		addr := addrBech
		if string(roleBytes) == vmcommon.DCDTRoleBurnForAll {
			addr = ""
		}

		args.tokenRolesAndProperties.AddRole(string(topics[tokenTopicsIndex]), addr, string(roleBytes), shouldAddRole)
	}

	return argOutputProcessEvent{
		processed: true,
	}
}

func (epp *dcdtPropertiesProc) extractDataNFTCreateRoleTransfer(args *argsProcessEvent) argOutputProcessEvent {
	topics := args.event.GetTopics()

	addrBech := epp.pubKeyConverter.SilentEncode(args.event.GetAddress(), log)
	shouldAddCreateRole := bytesToBool(topics[3])
	args.tokenRolesAndProperties.AddRole(string(topics[tokenTopicsIndex]), addrBech, core.DCDTRoleNFTCreate, shouldAddCreateRole)

	return argOutputProcessEvent{
		processed: true,
	}
}

func (epp *dcdtPropertiesProc) extractTokenProperties(args *argsProcessEvent) argOutputProcessEvent {
	topics := args.event.GetTopics()
	properties := topics[dcdtPropertiesStartIndex:]
	propertiesMap := make(map[string]bool)
	for i := 0; i < len(properties); i += propertyPairStep {
		property := string(properties[i])
		val := bytesToBool(properties[i+1])
		propertiesMap[property] = val
	}

	args.tokenRolesAndProperties.AddProperties(string(topics[tokenTopicsIndex]), propertiesMap)

	return argOutputProcessEvent{
		processed: true,
	}
}

func checkRolesBytes(rolesBytes [][]byte) bool {
	for _, role := range rolesBytes {
		if !containsNonLetterChars(string(role)) {
			return false
		}
	}

	return true
}

func containsNonLetterChars(data string) bool {
	for _, c := range data {
		if !unicode.IsLetter(c) {
			return false
		}
	}

	return true
}
