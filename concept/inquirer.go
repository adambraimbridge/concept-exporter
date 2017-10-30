package concept

import (
	"fmt"
	"github.com/Financial-Times/concept-exporter/db"
	log "github.com/sirupsen/logrus"
)

type State string

const (
	STARTING State = "Starting"
	RUNNING  State = "Running"
	FINISHED State = "Finished"
)

type Worker struct {
	ConceptCh   chan db.Concept `json:"-"`
	Errch       chan error      `json:"-"`
	ConceptType string          `json:"ConceptType,omitempty"`
	Count int                   `json:"Count,omitempty"`
	Progress int                `json:"Progress,omitempty"`
	Status   State              `json:"Status,omitempty"`
	ErrorMessage string         `json:"ErrorMessage,omitempty"`
}

type Inquirer interface {
	Inquire(tid string, candidates []string) []Worker
}

type NeoInquirer struct {
	Neo db.Service
}

func (n *NeoInquirer) Inquire(tid string, candidates []string) []Worker {
	var workers []Worker
	for _, cType := range candidates {
		worker := Worker{ConceptType: cType, Errch: make(chan error, 2), ConceptCh: make(chan db.Concept), Status: STARTING}
		workers = append(workers, worker)
	}
	go func() {
		log.WithField("transaction_id", tid).Infof("Starting reading concepts from Neo: %v", candidates)
		for _, worker := range workers {
			count, found, err := n.Neo.Read(worker.ConceptType, worker.ConceptCh)
			if err != nil {
				log.WithField("transaction_id", tid).Errorf("Error by reading %v concept type from Neo: %v", worker.ConceptType, err)
				worker.Errch <- err
				continue
			}
			if !found {
				log.WithField("transaction_id", tid).Errorf("Reading %v concept type from Neo returned empty result", worker.ConceptType)
				worker.Errch <- fmt.Errorf("Reading %v concept type from Neo returned empty result", worker.ConceptType)
				continue
			}
			log.WithField("transaction_id", tid).Infof("Found %v entries for %v concept", count, worker.ConceptType)
			worker.Count = count
		}
		log.WithField("transaction_id", tid).Info("Finished Neo read")
	}()
	return workers
}
