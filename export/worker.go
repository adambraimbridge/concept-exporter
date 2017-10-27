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
	Workers      []*concept.Worker `json:"ConceptWorkers,omitempty"`
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
	return *fe.job
}

func (fe *Service) CreateJob(candidates []string, errMsg string) Job {
	fe.job = &Job{ID: "job_" + uuid.New(), NrWorker: fe.NrOfConcurrentWorkers, Status: concept.STARTING, Concepts: candidates, ErrorMessage: errMsg, }

	return *fe.job
}

func (fe *Service) RunFullExport(tid string) {
	if fe.job == nil || fe.job.Status != concept.STARTING {
		log.WithField("transaction_id", tid).Error("No job to be run")
		return
	}

	log.Infof("Job started: %v", fe.job.ID)
	fe.job.Status = concept.RUNNING
	defer func() {
		fe.job.Status = concept.FINISHED
		log.Infof("Finished job %v with failed concept(s): %v, progress: %v", fe.job.ID, fe.job.Failed, fe.job.Progress)
	}()

	err := fe.Exporter.Prepare(fe.job.Concepts)
	if err != nil {
		log.WithField("transaction_id", tid).Errorf("Preparing CSV writer failed: %v", err.Error())
		fe.job.ErrorMessage = fmt.Sprintf("%s %s", fe.job.ErrorMessage, err.Error())
		return
	}

	fe.job.Workers = fe.Inquirer.Inquire(tid, fe.job.Concepts)

	for _, worker := range fe.job.Workers {
		fe.runExport(worker, tid)
	}

}
func (fe *Service) runExport(worker *concept.Worker, tid string) {
	worker.Status = concept.RUNNING
	defer func() {
		worker.Status = concept.FINISHED
	}()
	fe.job.Progress = append(fe.job.Progress, worker.ConceptType)
	for {
		select {
		case c, ok := <-worker.ConceptCh:
			if !ok {
				err := fe.Updater.Upload(fe.Exporter.GetBytes(worker.ConceptType), fe.Exporter.GetFileName(worker.ConceptType), tid)
				if err != nil {
					log.WithField("transaction_id", tid).Errorf("Upload to S3 Writer failed: %v", err)
					fe.job.Failed = append(fe.job.Failed, worker.ConceptType)
					worker.ErrorMessage = fmt.Sprintf("%s %s", worker.ErrorMessage, err.Error())
				}

				return
			}
			worker.Progress++
			fe.Exporter.Write(c, worker.ConceptType, tid)
		case err, ok := <-worker.Errch:
			if !ok {
				//channel closed
				return
			}
			fe.job.Failed = append(fe.job.Failed, worker.ConceptType)
			worker.ErrorMessage = fmt.Sprintf("%s %s", worker.ErrorMessage, err.Error())
		}
	}

}
