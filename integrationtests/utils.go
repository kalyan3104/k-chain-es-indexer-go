package integrationtests

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/kalyan3104/k-chain-core-go/core/pubkeyConverter"
	"github.com/kalyan3104/k-chain-es-indexer-go/client"
	"github.com/kalyan3104/k-chain-es-indexer-go/client/logging"
	"github.com/kalyan3104/k-chain-es-indexer-go/mock"
	"github.com/kalyan3104/k-chain-es-indexer-go/process/dataindexer"
	"github.com/kalyan3104/k-chain-es-indexer-go/process/elasticproc"
	"github.com/kalyan3104/k-chain-es-indexer-go/process/elasticproc/factory"
	logger "github.com/kalyan3104/k-chain-logger-go"
)

var (
	log                = logger.GetOrCreate("integration-tests")
	pubKeyConverter, _ = pubkeyConverter.NewBech32PubkeyConverter(32, addressPrefix)
)

// nolint
func setLogLevelDebug() {
	_ = logger.SetLogLevel("process:DEBUG")
}

// nolint
func createESClient(url string) (elasticproc.DatabaseClientHandler, error) {
	return client.NewElasticClient(elasticsearch.Config{
		Addresses: []string{url},
		Logger:    &logging.CustomLogger{},
	})
}

// nolint
func decodeAddress(address string) []byte {
	decoded, err := pubKeyConverter.Decode(address)
	log.LogIfError(err, "address", address)

	return decoded
}

// CreateElasticProcessor -
func CreateElasticProcessor(
	esClient elasticproc.DatabaseClientHandler,
) (dataindexer.ElasticProcessor, error) {
	args := factory.ArgElasticProcessorFactory{
		Marshalizer:              &mock.MarshalizerMock{},
		Hasher:                   &mock.HasherMock{},
		AddressPubkeyConverter:   pubKeyConverter,
		ValidatorPubkeyConverter: mock.NewPubkeyConverterMock(32),
		DBClient:                 esClient,
		EnabledIndexes: []string{dataindexer.TransactionsIndex, dataindexer.LogsIndex, dataindexer.AccountsDCDTIndex, dataindexer.ScResultsIndex,
			dataindexer.ReceiptsIndex, dataindexer.BlockIndex, dataindexer.AccountsIndex, dataindexer.TokensIndex, dataindexer.TagsIndex, dataindexer.EventsIndex,
			dataindexer.OperationsIndex, dataindexer.DelegatorsIndex, dataindexer.DCDTsIndex, dataindexer.SCDeploysIndex, dataindexer.MiniblocksIndex, dataindexer.ValuesIndex},
		Denomination: 18,
	}

	return factory.CreateElasticProcessor(args)
}

// nolint
func readExpectedResult(path string) string {
	jsonFile, _ := os.Open(path)
	byteValue, _ := ioutil.ReadAll(jsonFile)

	return string(byteValue)
}

// nolint
func getElementFromSlice(path string, index int) string {
	fileBytes := readExpectedResult(path)
	slice := make([]map[string]interface{}, 0)
	_ = json.Unmarshal([]byte(fileBytes), &slice)
	res, _ := json.Marshal(slice[index]["_source"])

	return string(res)
}

// nolint
func getIndexMappings(index string) (string, error) {
	u, _ := url.Parse(esURL)
	u.Path = path.Join(u.Path, index, "_mappings")
	res, err := http.Get(u.String())
	if err != nil {
		return "", err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}

	if res.StatusCode >= 400 {
		return "", fmt.Errorf("%s", string(body))
	}

	return string(body), nil
}
