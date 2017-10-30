package export

import (
	"github.com/Financial-Times/concept-exporter/concept"
	"sync"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"fmt"
)

type Job struct {
	sync.RWMutex
	wg           sync.WaitGroup
	NrWorker     int                `json:"-"`
	Workers      []concept.Worker `json:"ConceptWorkers,omitempty"`
	ID           string             `json:"ID"`
	Concepts     []string            `json:"Concepts,omitempty"`
	Progress     []string                `json:"Progress,omitempty"`
	Failed       []string           `json:"Failed,omitempty"`
	Status       concept.State              `json:"Status"`
	ErrorMessage string             `json:"ErrorMessage,omitempty"`
}

type Service struct {
	sync.RWMutex
	job                   *Job
	NrOfConcurrentWorkers int
	Updater               concept.Updater
	Inquirer              concept.Inquirer
	Exporter              *CsvExporter
}

func NewFullExporter(nrOfWorkers int, exporter concept.Updater, inquirer concept.Inquirer, csvExporter *CsvExporter) *Service {
	return &Service{
		NrOfConcurrentWorkers: nrOfWorkers,
		Updater:               exporter,
		Inquirer:              inquirer,
		Exporter:              csvExporter,
	}
}

func (fe *Service) IsRunningJob() bool {
	fe.Lock()
	defer fe.Unlock()
	if fe.job == nil {
		return false
	}
	return fe.job.Status == concept.RUNNING
}

func (fe *Service) GetCurrentJob() Job {
	fe.Lock()
	defer fe.Unlock()
	if fe.job == nil {
		return Job{}
	}
	return fe.getJob()
}

func (fe *Service) getJob() Job {
	workers := make([]concept.Worker, len(fe.job.Workers))
	for _, w := range fe.job.Workers {
		workers = append(workers, concept.Worker{
			ConceptType:  w.ConceptType,
			Progress:     w.Progress,
			Status:       w.Status,
			ErrorMessage: w.ErrorMessage,
			Count:        w.Count,
		})
	}
	return Job{
		ID:           fe.job.ID,
		Status:       fe.job.Status,
		ErrorMessage: fe.job.ErrorMessage,
		Concepts:     fe.job.Concepts,
		Progress:     fe.job.Progress,
		Failed:       fe.job.Failed,
		Workers:      workers,
	}
}

func (fe *Service) CreateJob(candidates []string, errMsg string) Job {
	fe.Lock()
	defer fe.Unlock()
	fe.job = &Job{ID: "job_" + uuid.New(), NrWorker: fe.NrOfConcurrentWorkers, Status: concept.STARTING, Concepts: candidates, ErrorMessage: errMsg, }
	return fe.getJob()
}

func (fe *Service) setJobStatus(state concept.State) {
	fe.Lock()
	defer fe.Unlock()
	fe.job.Status = state
}

func (fe *Service) setJobWorkers(workers []concept.Worker) {
	fe.Lock()
	defer fe.Unlock()
	fe.job.Workers = workers
}

func (fe *Service) setJobErrorMessage(msg string) {
	fe.Lock()
	defer fe.Unlock()
	fe.job.ErrorMessage = msg
}

func (fe *Service) setJobProgress(cType string) {
	fe.Lock()
	defer fe.Unlock()
	fe.job.Progress = append(fe.job.Progress, cType)
}

func (fe *Service) setJobFailed(cType string) {
	fe.Lock()
	defer fe.Unlock()
	fe.job.Failed = append(fe.job.Failed, cType)
}

func (fe *Service) RunFullExport(tid string) {
	if fe.job == nil || fe.job.Status != concept.STARTING {
		log.WithField("transaction_id", tid).Error("No job to be run")
		return
	}

	log.Infof("Job started: %v", fe.job.ID)
	fe.setJobStatus(concept.RUNNING)
	defer func() {
		fe.setJobStatus(concept.FINISHED)
		log.Infof("Finished job %v with failed concept(s): %v, progress: %v", fe.job.ID, fe.job.Failed, fe.job.Progress)
	}()

	err := fe.Exporter.Prepare(fe.job.Concepts)
	if err != nil {
		log.WithField("transaction_id", tid).Errorf("Preparing CSV writer failed: %v", err.Error())
		fe.setJobErrorMessage(fmt.Sprintf("%s %s", fe.job.ErrorMessage, err.Error()))
		return
	}

	fe.setJobWorkers(fe.Inquirer.Inquire(tid, fe.job.Concepts))

	for _, worker := range fe.job.Workers {
		fe.runExport(worker, tid)
	}
}

func (fe *Service) setWorkerState(worker *concept.Worker, state concept.State) {
	fe.Lock()
	defer fe.Unlock()
	worker.Status = state
}

func (fe *Service) setWorkerErrorMessage(worker *concept.Worker, msg string) {
	fe.Lock()
	defer fe.Unlock()
	worker.ErrorMessage = msg
}

func (fe *Service) incWorkerProgress(worker *concept.Worker) {
	fe.Lock()
	defer fe.Unlock()
	worker.Progress++
}

func (fe *Service) runExport(worker concept.Worker, tid string) {
	fe.setWorkerState(&worker, concept.RUNNING)
	defer func() {
		fe.setWorkerState(&worker, concept.FINISHED)
	}()
	fe.setJobProgress(worker.ConceptType)
	for {
		select {
		case c, ok := <-worker.ConceptCh:
			if !ok {
				err := fe.Updater.Upload(fe.Exporter.GetBytes(worker.ConceptType), fe.Exporter.GetFileName(worker.ConceptType), tid)
				if err != nil {
					log.WithField("transaction_id", tid).Errorf("Upload to S3 Writer failed: %v", err)
					fe.setJobFailed(worker.ConceptType)
					fe.setWorkerErrorMessage(&worker, fmt.Sprintf("%s %s", worker.ErrorMessage, err.Error()))
				}

				return
			}
			fe.incWorkerProgress(&worker)
			fe.Exporter.Write(c, worker.ConceptType, tid)
		case err, ok := <-worker.Errch:
			if !ok {
				//channel closed
				return
			}
			fe.setJobFailed(worker.ConceptType)
			fe.setWorkerErrorMessage(&worker, fmt.Sprintf("%s %s", worker.ErrorMessage, err.Error()))
		}
	}

}
