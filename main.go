package main

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Financial-Times/concept-exporter/concept"
	"github.com/Financial-Times/concept-exporter/db"
	"github.com/Financial-Times/concept-exporter/export"
	"github.com/Financial-Times/concept-exporter/web"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/http-handlers-go/v2/httphandlers"
	"github.com/Financial-Times/neo-utils-go/v2/neoutils"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	"github.com/sethgrid/pester"
)

const appDescription = "Exports concept from a data source (Neo4j) and sends it to S3"

func main() {
	app := cli.App("concept-exporter", appDescription)

	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  "concept-exporter",
		Desc:   "System Code of the application",
		EnvVar: "APP_SYSTEM_CODE",
	})
	appName := app.String(cli.StringOpt{
		Name:   "app-name",
		Value:  "concept-exporter",
		Desc:   "Application name",
		EnvVar: "APP_NAME",
	})
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})
	neoURL := app.String(cli.StringOpt{
		Name:   "neo-url",
		Value:  "http://localhost:7474/db/data",
		Desc:   "neo4j endpoint URL",
		EnvVar: "NEO_URL",
	})
	s3WriterBaseURL := app.String(cli.StringOpt{
		Name:   "s3WriterBaseURL",
		Value:  "http://localhost:8080",
		Desc:   "Base URL to S3 writer endpoint",
		EnvVar: "S3_WRITER_BASE_URL",
	})
	s3WriterHealthURL := app.String(cli.StringOpt{
		Name:   "s3WriterHealthURL",
		Value:  "http://localhost:8080/__gtg",
		Desc:   "Health URL to S3 writer endpoint",
		EnvVar: "S3_WRITER_HEALTH_URL",
	})
	conceptTypes := app.Strings(cli.StringsOpt{
		Name:   "conceptTypes",
		Value:  []string{"Brand", "Topic", "Location", "Person", "Organisation"},
		Desc:   "Concept types to support",
		EnvVar: "CONCEPT_TYPES",
	})
	logLevel := app.String(cli.StringOpt{
		Name:   "log-level",
		Value:  "info",
		Desc:   "Log level for the service",
		EnvVar: "LOG_LEVEL",
	})

	log := logger.NewUPPLogger("APP_NAME", *logLevel)

	app.Action = func() {
		log.WithField("service_name", *appName).Info("Service started")
		conf := neoutils.DefaultConnectionConfig()
		conf.HTTPClient.Timeout = 10 * time.Minute
		neoConn, err := neoutils.Connect(*neoURL, conf, log)

		if err != nil {
			log.Fatalf("Can't connect to neo4j, error=[%s]\n", err)
		}
		tr := &http.Transport{
			MaxIdleConnsPerHost: 128,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
		}
		c := &http.Client{
			Transport: tr,
			Timeout:   30 * time.Second,
		}
		client := pester.NewExtendedClient(c)
		client.Backoff = pester.ExponentialBackoff
		client.MaxRetries = 3
		client.Concurrency = 1

		uploader := &concept.S3Updater{Client: client, S3WriterBaseURL: *s3WriterBaseURL, S3WriterHealthURL: *s3WriterHealthURL}
		neoService := db.NewNeoService(neoConn, *neoURL)
		fullExporter := export.NewFullExporter(30, uploader, concept.NewNeoInquirer(neoService, log),
			export.NewCsvExporter(), log)

		healthService := newHealthService(
			&healthConfig{
				appSystemCode: *appSystemCode,
				appName:       *appName,
				port:          *port,
				s3Uploader:    uploader,
				neoService:    neoService,
				log:           log,
			})
		serveEndpoints(*appSystemCode, *appName, *port, web.NewRequestHandler(fullExporter, *conceptTypes, log), healthService, log)
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func serveEndpoints(appSystemCode string, appName string, port string, requestHandler *web.RequestHandler,
	healthService *healthService, log *logger.UPPLogger) {

	serveMux := http.NewServeMux()

	hc := health.HealthCheck{SystemCode: appSystemCode, Name: appName, Description: appDescription, Checks: healthService.checks}
	serveMux.HandleFunc(healthPath, health.Handler(hc))
	serveMux.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.GTG))
	serveMux.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	servicesRouter := mux.NewRouter()

	servicesRouter.HandleFunc("/export", requestHandler.Export).Methods(http.MethodPost)
	servicesRouter.HandleFunc("/job", requestHandler.GetJob).Methods(http.MethodGet)

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log, monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	serveMux.Handle("/", monitoringRouter)
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      serveMux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Infof("HTTP server closing with message: %v", err)
		}
		wg.Done()
	}()

	waitForSignal()
	log.Infof("[Shutdown] concept-exporter is shutting down")

	if err := server.Close(); err != nil {
		log.Errorf("Unable to stop HTTP server: %v", err)
	}
	wg.Wait()
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
