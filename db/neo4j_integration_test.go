// +build integration

package db

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Financial-Times/annotations-rw-neo4j/v3/annotations"
	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/concepts-rw-neo4j/concepts"
	"github.com/Financial-Times/content-rw-neo4j/content"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	contentUUID             = "a435b4ec-b207-4dce-ac0a-f8e7bbef310b"
	brandParentUUID         = "dbb0bdae-1f0c-1a1a-b0cb-b2227cce2b54"
	brandChildUUID          = "ff691bf8-8d92-1a1a-8326-c273400bff0b"
	brandGrandChildUUID     = "ff691bf8-8d92-2a2a-8326-c273400bff0b"
	financialInstrumentUUID = "77f613ad-1470-422c-bf7c-1dd4c3fd1693"
	companyUUID             = "eac853f5-3859-4c08-8540-55e043719400"
)

var allUUIDs = []string{contentUUID, brandParentUUID, brandChildUUID, brandGrandChildUUID, financialInstrumentUUID, companyUUID}

func getDatabaseConnection(t *testing.T) neoutils.NeoConnection {
	if testing.Short() {
		t.Skip("Neo4j integration for long tests only.")
	}
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	conn, err := neoutils.Connect(url, conf)
	assert.NoError(t, err, "Failed to connect to Neo4j")
	return conn
}

func TestNeoService_ReadBrand(t *testing.T) {
	conn := getDatabaseConnection(t)
	svc := concepts.NewConceptService(conn)
	cleanDB(t, conn)
	writeBrands(t, svc)
	writeContent(t, conn)
	writeAnnotation(t, conn, fmt.Sprintf("./fixtures/Annotations-%s.json", contentUUID), "v1")

	neoSvc := NewNeoService(conn, "not-needed")

	conceptCh := make(chan Concept)
	count, found, err := neoSvc.Read("Brand", conceptCh)

	assert.NoError(t, err, "Error reading from Neo")
	assert.True(t, found)
	assert.Equal(t, 1, count)
waitLoop:
	for {
		select {
		case c, open := <-conceptCh:
			if !open {
				break waitLoop
			}
			assert.Equal(t, "ff691bf8-8d92-1a1a-8326-c273400bff0b", c.Uuid)
			assert.Equal(t, "http://api.ft.com/things/ff691bf8-8d92-1a1a-8326-c273400bff0b", c.Id)
			assert.Equal(t, "http://api.ft.com/brands/ff691bf8-8d92-1a1a-8326-c273400bff0b", c.ApiUrl)
			assert.Equal(t, "Business School video", c.PrefLabel)
			assertListContainsAll(t, []string{"Thing", "Concept", "Brand", "Classification"}, c.Labels)
			assert.Empty(t, c.LeiCode)
			assert.Empty(t, c.FIGI)
		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
}

func TestNeoService_ReadHasBrand(t *testing.T) {
	conn := getDatabaseConnection(t)
	svc := concepts.NewConceptService(conn)
	cleanDB(t, conn)
	writeBrands(t, svc)
	writeContent(t, conn)
	writeAnnotation(t, conn, fmt.Sprintf("./fixtures/Annotations-%s-hasBrand.json", contentUUID), "v1")

	neoSvc := NewNeoService(conn, "not-needed")

	conceptCh := make(chan Concept)
	count, found, err := neoSvc.Read("Brand", conceptCh)

	assert.NoError(t, err, "Error reading from Neo")
	assert.True(t, found)
	assert.Equal(t, 1, count)
waitLoop:
	for {
		select {
		case c, open := <-conceptCh:
			if !open {
				break waitLoop
			}
			assert.Equal(t, "ff691bf8-8d92-1a1a-8326-c273400bff0b", c.Uuid)
			assert.Equal(t, "http://api.ft.com/things/ff691bf8-8d92-1a1a-8326-c273400bff0b", c.Id)
			assert.Equal(t, "http://api.ft.com/brands/ff691bf8-8d92-1a1a-8326-c273400bff0b", c.ApiUrl)
			assert.Equal(t, "Business School video", c.PrefLabel)
			assertListContainsAll(t, []string{"Thing", "Concept", "Brand", "Classification"}, c.Labels)
			assert.Empty(t, c.LeiCode)
			assert.Empty(t, c.FIGI)
		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
}

func TestNeoService_ReadOrganisation(t *testing.T) {
	conn := getDatabaseConnection(t)
	svc := concepts.NewConceptService(conn)
	cleanDB(t, conn)
	writeOrganisations(t, svc)
	writeFinancialInstruments(t, svc)
	writeContent(t, conn)
	writeAnnotation(t, conn, fmt.Sprintf("./fixtures/Annotations-%s-org.json", contentUUID), "v2")
	neoSvc := NewNeoService(conn, "not-needed")

	conceptCh := make(chan Concept)
	count, found, err := neoSvc.Read("Organisation", conceptCh)

	assert.NoError(t, err, "Error reading from Neo")
	assert.True(t, found)
	assert.Equal(t, 1, count)
waitLoop:
	for {
		select {
		case c, open := <-conceptCh:
			if !open {
				break waitLoop
			}
			assert.Equal(t, "eac853f5-3859-4c08-8540-55e043719400", c.Uuid)
			assert.Equal(t, "http://api.ft.com/things/eac853f5-3859-4c08-8540-55e043719400", c.Id)
			assert.Equal(t, "http://api.ft.com/organisations/eac853f5-3859-4c08-8540-55e043719400", c.ApiUrl)
			assert.Equal(t, "Fakebook", c.PrefLabel)
			assertListContainsAll(t, []string{"Thing", "Concept", "Organisation", "PublicCompany", "Company"}, c.Labels)
			assert.Equal(t, "PBLD0EJDB5FWOLXP3B76", c.LeiCode)
			assert.Equal(t, "BB8000C3P0-R2D2", c.FIGI)
		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
}

func TestNeoService_ReadWithoutResult(t *testing.T) {
	conn := getDatabaseConnection(t)
	cleanDB(t, conn)
	neoSvc := NewNeoService(conn, "not-needed")

	conceptCh := make(chan Concept)
	count, found, err := neoSvc.Read("Brand", conceptCh)

	assert.NoError(t, err, "Error reading from Neo")
	assert.False(t, found)
	assert.Equal(t, 0, count)
waitLoop:
	for {
		select {
		case _, open := <-conceptCh:
			if !open {
				break waitLoop
			}
			t.FailNow()
		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
}

type interceptingCypherConn struct {
	db         neoutils.NeoConnection
	shouldFail bool
}

func (c interceptingCypherConn) CypherBatch(cypher []*neoism.CypherQuery) error {
	if c.shouldFail {
		return fmt.Errorf("BOOM!")
	}
	return c.db.CypherBatch(cypher)
}

func (c interceptingCypherConn) EnsureConstraints(constraints map[string]string) error {
	return c.db.EnsureConstraints(constraints)
}

func (c interceptingCypherConn) EnsureIndexes(indexes map[string]string) error {
	return c.db.EnsureIndexes(indexes)
}

func TestNeoService_ReadWithError(t *testing.T) {
	conn := &interceptingCypherConn{db: getDatabaseConnection(t), shouldFail: true}
	neoSvc := NewNeoService(conn, "not-needed")

	conceptCh := make(chan Concept)
	count, found, err := neoSvc.Read("Brand", conceptCh)

	assert.Error(t, err, "Error reading from Neo")
	assert.Equal(t, "BOOM!", err.Error())
	assert.False(t, found)
	assert.Equal(t, 0, count)
waitLoop:
	for {
		select {
		case _, open := <-conceptCh:
			if !open {
				break waitLoop
			}
			t.FailNow()
		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
}

func assertListContainsAll(t *testing.T, list interface{}, items ...interface{}) {
	if reflect.TypeOf(items[0]).Kind().String() == "slice" {
		expected := reflect.ValueOf(items[0])
		expectedLength := expected.Len()
		for i := 0; i < expectedLength; i++ {
			assert.Contains(t, list, expected.Index(i).Interface())
		}
	} else {
		for _, item := range items {
			assert.Contains(t, list, item)
		}
	}
}

func writeAnnotation(t *testing.T, conn neoutils.NeoConnection, pathToJson, platform string) {
	annrw := annotations.NewCypherAnnotationsService(conn)
	assert.NoError(t, annrw.Initialise())
	writeJSONToAnnotationService(t, annrw, pathToJson, contentUUID, platform)
}

func writeContent(t *testing.T, conn neoutils.NeoConnection) {
	contentRW := content.NewCypherContentService(conn)
	require.NoError(t, contentRW.Initialise())
	writeJSONToContentService(t, contentRW, fmt.Sprintf("./fixtures/Content-%s.json", contentUUID))
}

func writeBrands(t *testing.T, service concepts.ConceptService) concepts.ConceptService {
	assert.NoError(t, service.Initialise())
	writeJSONToConceptService(t, service, fmt.Sprintf("./fixtures/Brand-%s-parent.json", brandParentUUID))
	writeJSONToConceptService(t, service, fmt.Sprintf("./fixtures/Brand-%s-child.json", brandChildUUID))
	writeJSONToConceptService(t, service, fmt.Sprintf("./fixtures/Brand-%s-grand_child.json", brandGrandChildUUID))
	return service
}

func writeOrganisations(t *testing.T, service concepts.ConceptService) concepts.ConceptService {
	assert.NoError(t, service.Initialise())
	writeJSONToConceptService(t, service, fmt.Sprintf("./fixtures/Organisation-Fakebook-%s.json", companyUUID))
	return service
}

func writeFinancialInstruments(t *testing.T, service concepts.ConceptService) concepts.ConceptService {
	assert.NoError(t, service.Initialise())
	writeJSONToConceptService(t, service, fmt.Sprintf("./fixtures/FinancialInstrument-%s.json", financialInstrumentUUID))
	return service
}

func writeJSONToConceptService(t *testing.T, service concepts.ConceptService, pathToJsonFile string) {
	f, err := os.Open(pathToJsonFile)
	require.NoError(t, err)
	dec := json.NewDecoder(f)
	inst, _, err := service.DecodeJSON(dec)
	require.NoError(t, err)
	_, err = service.Write(inst, "trans_id")
	require.NoError(t, err)
	f.Close()
}

func writeJSONToContentService(t *testing.T, service baseftrwapp.Service, pathToJsonFile string) {
	f, err := os.Open(pathToJsonFile)
	require.NoError(t, err)
	dec := json.NewDecoder(f)
	inst, _, err := service.DecodeJSON(dec)
	require.NoError(t, err)
	require.NoError(t, service.Write(inst, "trans_id"))
	f.Close()
}

func writeJSONToAnnotationService(t *testing.T, service annotations.Service, pathToJsonFile, uuid, platform string) {
	f, err := os.Open(pathToJsonFile)
	require.NoError(t, err)
	dec := json.NewDecoder(f)
	inst, err := service.DecodeJSON(dec)
	require.NoError(t, err)
	require.NoError(t, service.Write(uuid, fmt.Sprintf("annotations-%s", platform), platform, "trans_id", inst))
	f.Close()
}

//DELETES ALL DATA! DO NOT USE IN PRODUCTION!!!
func cleanDB(t *testing.T, db neoutils.NeoConnection) {
	qs := make([]*neoism.CypherQuery, len(allUUIDs))
	for i, uuid := range allUUIDs {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MATCH (a:Thing{uuid:"%s"})
			OPTIONAL MATCH (a)<-[:IDENTIFIES]-(i:Identifier)
			OPTIONAL MATCH (a)-[:EQUIVALENT_TO]-(t:Thing)
			OPTIONAL MATCH (a)-[r]-()
			DETACH DELETE i, t, a, r`, uuid),
		}
	}
	err := db.CypherBatch(qs)
	assert.NoError(t, err)
}
