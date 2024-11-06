package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	checkNil "github.com/kalyan3104/k-chain-core-go/core/check"
	"github.com/kalyan3104/k-chain-core-go/core/closing"
	"github.com/kalyan3104/k-chain-es-indexer-go/tools/accounts-balance-checker/pkg/check"
	"github.com/kalyan3104/k-chain-es-indexer-go/tools/accounts-balance-checker/pkg/config"
	logger "github.com/kalyan3104/k-chain-logger-go"
	"github.com/kalyan3104/k-chain-logger-go/file"
	"github.com/urfave/cli"
)

const (
	defaultLogsPath           = "logs"
	logsFileLifeSpamInSeconds = 432000
	logsFileMaxSizeInMbs      = 1024
)

var (
	log = logger.GetOrCreate("main")

	configFile = cli.StringFlag{
		Name:  "config-file",
		Value: "config.json",
	}
	checkBalanceREWA = cli.BoolFlag{
		Name:  "check-balance-rewa",
		Usage: "If set, the checker will verify all the balance value of the accounts with REWA",
	}
	checkBalanceDCDT = cli.BoolFlag{
		Name:  "check-balance-dcdt",
		Usage: "If set, the checker wil verify all the balance value of the accounts with DCDT",
	}
	repairFlag = cli.BoolFlag{
		Name:  "repair",
		Usage: "If set, the checker will also repair the wrong balances",
	}

	logLevel = cli.StringFlag{
		Name: "log-level",
		Usage: "This flag specifies the logger `level(s)`. It can contain multiple comma-separated value. For example" +
			", if set to *:INFO the logs for all packages will have the INFO level. However, if set to *:INFO,api:DEBUG" +
			" the logs for all packages will have the INFO level, excepting the api package which will receive a DEBUG" +
			" log level.",
		Value: "*:" + logger.LogInfo.String(),
	}
	logSaveFile = cli.BoolFlag{
		Name:  "log-save",
		Usage: "Boolean option for enabling log saving. If set, it will automatically save all the logs into a file.",
	}
	// enableAnsiColor defines if the logger subsystem should displaying ANSI colors
	enableAnsiColor = cli.BoolFlag{
		Name:  "enable-ansi-color",
		Usage: "Boolean option for enable ANSI colors in the logging system.",
	}
)

func main() {
	app := cli.NewApp()
	app.Name = "Elasticsearch accounts balance checker"
	app.Version = "v1.0.0"
	app.Usage = "This is the entry point for Elasticsearch accounts balance checker tool"
	app.Flags = []cli.Flag{
		configFile,
		checkBalanceREWA,
		checkBalanceDCDT,
		logLevel,
		repairFlag,
		logSaveFile,
		enableAnsiColor,
	}
	app.Authors = []cli.Author{
		{
			Name:  "The kalyan Team",
			Email: "contact@kalyan.com",
		},
	}

	app.Action = startCheck
	err := app.Run(os.Args)
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}

func startCheck(ctx *cli.Context) {
	fileLogging, err := initializeLogger(ctx)
	if err != nil {
		log.Error("cannot initialize logger", "error", err)
		return
	}

	cfg, err := readConfig(ctx)
	if err != nil {
		log.Error("cannot read config file", "error", err)
		return
	}

	repair := ctx.Bool(repairFlag.Name)

	balanceChecker, err := check.CreateBalanceChecker(cfg, repair)
	if err != nil {
		log.Error("cannot create balance checker", "error", err)
		return
	}

	shouldCheckBalanceREWA := ctx.Bool(checkBalanceREWA.Name)
	if shouldCheckBalanceREWA {
		err = balanceChecker.CheckREWABalances()
		if err != nil {
			log.Error("cannot check balance REWA", "error", err)
			return
		}

		log.Info("done")
	}

	shouldCheckBalanceDCDT := ctx.Bool(checkBalanceDCDT.Name)
	if shouldCheckBalanceDCDT {
		err = balanceChecker.CheckDCDTBalances()
		if err != nil {
			log.Error("cannot check balance DCDT", "error", err)
			return
		}
	}

	if !shouldCheckBalanceREWA && !shouldCheckBalanceDCDT {
		log.Error("no flag has been provided")
	}

	if checkNil.IfNilReflect(fileLogging) {
		err = fileLogging.Close()
		log.LogIfError(err)
	}

	return
}

func readConfig(ctx *cli.Context) (*config.Config, error) {
	jsonFile, err := ioutil.ReadFile(ctx.String(configFile.Name))
	if err != nil {
		return nil, err
	}
	cfg := &config.Config{}
	err = json.Unmarshal(jsonFile, cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func initializeLogger(ctx *cli.Context) (closing.Closer, error) {
	logLevelFlagValue := ctx.GlobalString(logLevel.Name)
	err := logger.SetLogLevel(logLevelFlagValue)
	if err != nil {
		return nil, err
	}

	withLogFile := ctx.GlobalBool(logSaveFile.Name)
	if !withLogFile {
		return nil, nil
	}

	workingDir, err := os.Getwd()
	if err != nil {
		log.LogIfError(err)
		workingDir = ""
	}

	fileLogging, err := file.NewFileLogging(file.ArgsFileLogging{
		WorkingDir:      workingDir,
		DefaultLogsPath: defaultLogsPath,
		LogFilePrefix:   "",
	})
	if err != nil {
		return nil, fmt.Errorf("%w creating a log file", err)
	}

	err = fileLogging.ChangeFileLifeSpan(
		time.Second*time.Duration(logsFileLifeSpamInSeconds),
		uint64(logsFileMaxSizeInMbs),
	)
	if err != nil {
		return nil, err
	}

	enableAnsi := ctx.GlobalBool(enableAnsiColor.Name)
	err = removeANSIColorsForLoggerIfNeeded(enableAnsi)
	if err != nil {
		return nil, err
	}

	return fileLogging, nil
}

func removeANSIColorsForLoggerIfNeeded(enableAnsi bool) error {
	if enableAnsi {
		return nil
	}

	err := logger.RemoveLogObserver(os.Stdout)
	if err != nil {
		return err
	}

	return logger.AddLogObserver(os.Stdout, &logger.PlainFormatter{})
}
