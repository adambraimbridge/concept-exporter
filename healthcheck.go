package main

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/Financial-Times/concept-exporter/concept"
	"github.com/Financial-Times/concept-exporter/db"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/Financial-Times/service-status-go/gtg"
)

const healthPath = "/__health"

type healthService struct {
	config *healthConfig
	checks []health.Check
}

type healthConfig struct {
	appSystemCode string
	appName       string
	port          string
	s3Uploader    *concept.S3Updater
	neoService    *db.NeoService
}

func newHealthService(config *healthConfig) *healthService {
	svc := &healthService{config: config}
	svc.checks = []health.Check{
		svc.NeoCheck(),
		svc.S3WriterCheck(),
	}
	return svc
}

func (service *healthService) NeoCheck() health.Check {
	conf := neoutils.DefaultConnectionConfig()
	conf.HTTPClient.Timeout = 5 * time.Second
	conn, connErr := neoutils.Connect(service.config.neoService.NeoURL, conf)
	return health.Check{
		Name:             "CheckConnectivityToNeo4j",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/concept-exporter.html",
		Severity:         2,
		TechnicalSummary: fmt.Sprintf("The service is unable to connect to Neo4j (%s). Export won't work because of this", service.config.neoService.NeoURL),
		Checker: func() (string, error) {
			if connErr != nil {
				return "Could not make initial connection to Neo", connErr
			}
			return service.config.neoService.CheckConnectivity(conn)
		},
	}
}

func (service *healthService) S3WriterCheck() health.Check {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 10,
		Dial: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 3 * time.Second,
		}).Dial,
	}
	httpClient := &http.Client{
		Transport: tr,
		Timeout:   3 * time.Second,
	}
	return health.Check{
		Name:             "CheckConnectivityToExportRWS3",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/concept-exporter.html",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to Export-RW-S3. Export won't work because of this",
		Checker: func() (string, error) {
			return service.config.s3Uploader.CheckHealth(httpClient)
		},
	}
}

func (service *healthService) GTG() gtg.Status {
	s3WriterCheck := func() gtg.Status {
		return service.gtgCheck(service.S3WriterCheck())
	}
	neoCheck := func() gtg.Status {
		return service.gtgCheck(service.NeoCheck())
	}

	return gtg.FailFastParallelCheck([]gtg.StatusChecker{
		s3WriterCheck,
		neoCheck,
	})()
}

func (service *healthService) gtgCheck(check health.Check) gtg.Status {
	if _, err := check.Checker(); err != nil {
		return gtg.Status{GoodToGo: false, Message: err.Error()}
	}
	return gtg.Status{GoodToGo: true}
}
