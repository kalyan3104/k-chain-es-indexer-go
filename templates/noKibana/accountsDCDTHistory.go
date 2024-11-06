package noKibana

// AccountsDCDTHistory will hold the configuration for the accountsdcdthistory index
var AccountsDCDTHistory = Object{
	"index_patterns": Array{
		"accountsdcdthistory-*",
	},
	"settings": Object{
		"number_of_shards":   5,
		"number_of_replicas": 0,
	},
	"mappings": Object{
		"properties": Object{
			"address": Object{
				"type": "keyword",
			},
			"balance": Object{
				"type": "keyword",
			},
			"identifier": Object{
				"type": "text",
			},
			"isSender": Object{
				"type": "boolean",
			},
			"isSmartContract": Object{
				"type": "boolean",
			},
			"shardID": Object{
				"type": "long",
			},
			"timestamp": Object{
				"type":   "date",
				"format": "epoch_second",
			},
			"token": Object{
				"type": "text",
			},
			"tokenNonce": Object{
				"type": "double",
			},
		},
	},
}
