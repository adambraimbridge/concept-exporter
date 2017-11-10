package main

import (
	"fmt"
	"github.com/Financial-Times/concept-exporter/concept"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/Financial-Times/service-status-go/gtg"
	log "github.com/sirupsen/logrus"
	"time"
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
	NeoURL        string
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
	conn, err := neoutils.Connect(service.config.NeoURL, conf)
	log.Infof("New connection for Neo for health check: %v, with error: %v", conn, err)
	return health.Check{
		Name:             "CheckConnectivityToNeo4j",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/concept-exporter.html",
		Severity:         2,
		TechnicalSummary: fmt.Sprintf("The service is unable to connect to Neo4j (%s). Export won't work because of this", service.config.NeoURL),
		Checker: func() (string, error) {
			err := neoutils.Check(conn)
			if err != nil {
				return "Could not connect to Neo", err
			}
			return "Neo could be reached", nil
		},
	}
}

func (service *healthService) S3WriterCheck() health.Check {
	return health.Check{
		Name:             "CheckConnectivityToExportRWS3",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/concept-exporter.html",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to Export-RW-S3. Export won't work because of this",
		Checker:          service.config.s3Uploader.CheckHealth,
	}
}

func (service *healthService) gtgCheck() gtg.Status {
	for _, check := range service.checks {
		if _, err := check.Checker(); err != nil {
			return gtg.Status{GoodToGo: false, Message: err.Error()}
		}
	}
	return gtg.Status{GoodToGo: true}
}
