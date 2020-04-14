package web

import (
	"encoding/json"
	"fmt"

	"io/ioutil"
	"net/http"
	"strings"

	"github.com/Financial-Times/concept-exporter/export"
	logger "github.com/Financial-Times/go-logger/v2"
	transactionidutils "github.com/Financial-Times/transactionid-utils-go"
)

type RequestHandler struct {
	Exporter     *export.FullExporter
	ConceptTypes []string
	Log          *logger.UPPLogger
}

func NewRequestHandler(fullExporter *export.FullExporter, conceptTypes []string, log *logger.UPPLogger) *RequestHandler {
	return &RequestHandler{
		Exporter:     fullExporter,
		ConceptTypes: conceptTypes,
		Log:          log,
	}
}

func (handler *RequestHandler) GetJob(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Add("Content-Type", "application/json")

	job := handler.Exporter.GetCurrentJob()

	err := json.NewEncoder(writer).Encode(&job)
	if err != nil {
		msg := fmt.Sprintf(`Failed to write job %v to response writer: "%v"`, job.ID, err)
		tid := transactionidutils.GetTransactionIDFromRequest(request)
		handler.Log.WithTransactionID(tid).Warn(msg)
		fmt.Fprintf(writer, "{\"ID\": \"%v\"}", job.ID)
		return
	}
}

func (handler *RequestHandler) Export(writer http.ResponseWriter, request *http.Request) {
	tid := transactionidutils.GetTransactionIDFromRequest(request)

	if handler.Exporter.IsRunningJob() {
		http.Error(writer, "There are already running export jobs. Please wait them to finish", http.StatusBadRequest)
		return
	}
	candidates, errMsg := handler.getCandidateConceptTypes(request, tid)
	if len(candidates) == 0 {
		http.Error(writer, "No valid candidate concept types in the request", http.StatusBadRequest)
		return
	}
	job := handler.Exporter.CreateJob(candidates, errMsg)
	go handler.Exporter.RunFullExport(tid)
	writer.WriteHeader(http.StatusAccepted)
	writer.Header().Add("Content-Type", "application/json")

	err := json.NewEncoder(writer).Encode(&job)
	if err != nil {
		msg := fmt.Sprintf(`Failed to write job %v to response writer: "%v"`, job.ID, err)
		handler.Log.WithTransactionID(tid).Warnf(msg)
		fmt.Fprintf(writer, "{\"ID\": \"%v\"}", job.ID)
		return
	}
}

func (handler *RequestHandler) getCandidateConceptTypes(request *http.Request, tid string) (candidates []string, errMsg string) {
	candidates = extractCandidateConceptTypesFromRequest(request, handler.Log.WithTransactionID(tid))
	if len(candidates) != 0 {
		var unsupported []string
		for i, cand := range candidates {
			found := false
			for _, cType := range handler.ConceptTypes {
				if cand == cType {
					found = true
					break
				}
			}
			if !found {
				unsupported = append(unsupported, cand)
				candidates = append(candidates[:i], candidates[i+1:]...)
			}
		}
		if len(unsupported) != 0 {
			errMsg = fmt.Sprintf("There are unsupported concept types within the candidates: %v", unsupported)
		}
		if len(candidates) == 0 {
			return
		}
	}
	if len(candidates) == 0 {
		handler.Log.WithTransactionID(tid).Infof("Content type candidates are empty. Using all supported ones: %v", handler.ConceptTypes)
		candidates = handler.ConceptTypes
	}
	return
}

func extractCandidateConceptTypesFromRequest(request *http.Request, log *logger.LogEntry) (candidates []string) {
	var result map[string]interface{}
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		log.WithError(err).Error("no valid POST body found, thus no candidate concept types to export")
		return
	}

	if err = json.Unmarshal(body, &result); err != nil {
		log.WithError(err).Error("no valid JSON body found, thus no candidate concept types to export")
		return
	}
	log.Debugf("Parsing request body: %v", result)
	cTypes, ok := result["conceptTypes"]
	if !ok {
		log.Infof("no conceptTypes field found in the JSON body, thus no candidate concept types to export.")
		return
	}
	cTypesString, ok := cTypes.(string)
	if ok {
		candidates = strings.Split(cTypesString, " ")
	} else {
		log.WithError(err).Error("the conceptTypes field found in JSON body is not a string as expected.")
	}
	return
}
