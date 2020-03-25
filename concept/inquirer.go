package concept

import (
	"fmt"
	"sync"

	"github.com/Financial-Times/concept-exporter/db"
	"github.com/Financial-Times/go-logger/v2"
)

type State string

const (
	STARTING State = "Starting"
	RUNNING  State = "Running"
	FINISHED State = "Finished"
)

type Worker struct {
	sync.RWMutex
	ConceptCh    chan db.Concept `json:"-"`
	Errch        chan error      `json:"-"`
	ConceptType  string          `json:"ConceptType,omitempty"`
	Count        int             `json:"Count,omitempty"`
	Progress     int             `json:"Progress,omitempty"`
	Status       State           `json:"Status,omitempty"`
	ErrorMessage string          `json:"ErrorMessage,omitempty"`
}

func (w *Worker) setCount(count int) {
	w.Lock()
	defer w.Unlock()
	w.Count = count
}

func (w *Worker) GetCount() int {
	w.Lock()
	defer w.Unlock()
	return w.Count
}

type Inquirer interface {
	Inquire(candidates []string, tid string) []*Worker
}

type NeoInquirer struct {
	Neo db.Service
	Log *logger.UPPLogger
}

func NewNeoInquirer(neo db.Service, log *logger.UPPLogger) *NeoInquirer {
	return &NeoInquirer{Neo: neo, Log: log}
}

func (n *NeoInquirer) Inquire(candidates []string, tid string) []*Worker {
	var workers []*Worker
	for _, cType := range candidates {
		worker := &Worker{ConceptType: cType, Errch: make(chan error, 2), ConceptCh: make(chan db.Concept), Status: STARTING}
		workers = append(workers, worker)
	}
	go func() {
		logEntry := n.Log.WithTransactionID(tid)
		logEntry.Infof("Starting reading concepts from Neo: %v", candidates)
		for _, worker := range workers {
			count, found, err := n.Neo.Read(worker.ConceptType, worker.ConceptCh)
			if err != nil {
				logEntry.WithError(err).Errorf("error by reading %v concept type from Neo", worker.ConceptType)
				worker.Errch <- err
				continue
			}
			if !found {
				err = fmt.Errorf("reading %v concept type from Neo returned empty result", worker.ConceptType)
				logEntry.Error(err)
				worker.Errch <- err
				continue
			}
			logEntry.Infof("Found %v entries for %v concept", count, worker.ConceptType)
			worker.setCount(count)
		}
		logEntry.Info("Finished Neo read")
	}()
	return workers
}
