package db

import (
	"fmt"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

//Service reads from a data source and uses a channel to iterate on the retrieved values for the given concept type
type Service interface {
	Read(conceptType string, conceptCh chan Concept) (int, bool, error)
}

//NeoService is the implementation of Service for Neo4j
type NeoService struct {
	Connection neoutils.NeoConnection
	NeoURL     string
}

//Returns a new NeoService
func NewNeoService(conn neoutils.NeoConnection, neoURL string) *NeoService {
	return &NeoService{Connection: conn, NeoURL: neoURL}
}

//Concept is the model for the data read from the data source
type Concept struct {
	Id        string
	Uuid      string
	PrefLabel string
	ApiUrl    string
	Labels    []string
	LeiCode   string
	FactsetId string
	FIGI      string
}

func (s *NeoService) Read(conceptType string, conceptCh chan Concept) (int, bool, error) {
	results := []Concept{}
	stmt := fmt.Sprintf(`
			MATCH (c:%s)-[:MENTIONS|MAJOR_MENTIONS|ABOUT|IS_CLASSIFIED_BY|IS_PRIMARILY_CLASSIFIED_BY|HAS_AUTHOR]-(cc:Content)
			MATCH (c)-[:EQUIVALENT_TO]->(x:Thing)
			RETURN DISTINCT x.prefUUID AS Uuid, x.prefLabel AS PrefLabel, labels(c) AS Labels
		`, conceptType)

	if conceptType == "Organisation" {
		stmt = `
		MATCH (content:Content)-[rel:MENTIONS|MAJOR_MENTIONS|ABOUT|IS_CLASSIFIED_BY|IS_PRIMARILY_CLASSIFIED_BY|HAS_AUTHOR]->(concept:Organisation)
		OPTIONAL MATCH (concept)-[:EQUIVALENT_TO]->(x:Thing)
		OPTIONAL MATCH (concept)<-[:IDENTIFIES]-(factset:FactsetIdentifier)
		OPTIONAL MATCH (concept)<-[:IDENTIFIES]-(lei:LegalEntityIdentifier)
		OPTIONAL MATCH (concept)<-[:ISSUED_BY]-(:FinancialInstrument)<-[:IDENTIFIES]-(figi:FIGIIdentifier)
		RETURN DISTINCT coalesce(x.prefUUID, concept.uuid) as Uuid, coalesce(labels(x), labels(concept)) as Labels,
                coalesce(x.prefLabel, concept.prefLabel) as PrefLabel, coalesce(x.factsetId,factset.value) as factsetId, coalesce(x.leiCode, lei.value) as leiCode, coalesce(x.figiCode, figi.value) as FIGI
		`
	}
	if conceptType == "Person" {
		stmt = `
		MATCH (content:Content)-[rel:MENTIONS|MAJOR_MENTIONS|ABOUT|IS_CLASSIFIED_BY|IS_PRIMARILY_CLASSIFIED_BY|HAS_AUTHOR]->(concept:Person)
		OPTIONAL MATCH (concept)-[:EQUIVALENT_TO]->(x:Thing)
		RETURN DISTINCT coalesce(x.prefUUID, concept.uuid) as Uuid, coalesce(labels(x), labels(concept)) as Labels,
                coalesce(x.prefLabel, concept.prefLabel) as PrefLabel
		`
	}

	query := &neoism.CypherQuery{
		Statement: stmt,
		Result:    &results,
	}

	err := s.Connection.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		close(conceptCh)
		return 0, false, err
	}
	if len(results) == 0 {
		close(conceptCh)
		return 0, false, nil
	}
	go func() {
		defer close(conceptCh)
		for _, c := range results {
			c.ApiUrl = mapper.APIURL(c.Uuid, c.Labels, "")
			c.Id = mapper.IDURL(c.Uuid)
			conceptCh <- c
		}
	}()
	return len(results), true, nil
}

func (s *NeoService) CheckConnectivity(conn neoutils.NeoConnection) (string, error) {
	err := neoutils.Check(conn)
	if err != nil {
		return "Could not connect to Neo", err
	}
	return "Neo could be reached", nil
}
