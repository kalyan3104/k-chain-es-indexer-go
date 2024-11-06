package factory

import (
	"github.com/kalyan3104/k-chain-es-indexer-go/api/gin"
	"github.com/kalyan3104/k-chain-es-indexer-go/config"
	"github.com/kalyan3104/k-chain-es-indexer-go/core"
	"github.com/kalyan3104/k-chain-es-indexer-go/facade"
)

// CreateWebServer will create a new instance of core.WebServerHandler
func CreateWebServer(apiConfig config.ApiRoutesConfig, statusMetricsHandler core.StatusMetricsHandler) (core.WebServerHandler, error) {
	metricsFacade, err := facade.NewMetricsFacade(statusMetricsHandler)
	if err != nil {
		return nil, err
	}

	args := gin.ArgsWebServer{
		Facade:    metricsFacade,
		ApiConfig: apiConfig,
	}
	return gin.NewWebServer(args)
}
