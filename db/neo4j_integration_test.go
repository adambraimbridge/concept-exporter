// +build integration

package db

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/Financial-Times/annotations-rw-neo4j/v3/annotations"
	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/concepts-rw-neo4j/concepts"
	"github.com/Financial-Times/content-rw-neo4j/content"
	logger "github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/neo-utils-go/v2/neoutils"
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
	organisationUUID        = "5d1510f8-2779-4b74-adab-0a5eb138fca6"
	personUUID              = "b2fa511e-a031-4d52-b37d-72fd290b39ce"
	personWithBrandUUID     = "9070a3f1-aa6d-48a7-9d97-f56a47513cef"
)

var allUUIDs = []string{contentUUID, brandParentUUID, brandChildUUID, brandGrandChildUUID, financialInstrumentUUID, companyUUID, organisationUUID, personUUID, personWithBrandUUID}

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
	conn, err := neoutils.Connect(url, conf, logger.NewUPPLogger("test-concept-exporter", "PANIC"))
	assert.NoError(t, err, "Failed to connect to Neo4j")
	return conn
}

func TestNeoService_ReadBrand(t *testing.T) {
	conn := getDatabaseConnection(t)
	svc := concepts.NewConceptService(conn)
	assert.NoError(t, svc.Initialise())

	cleanDB(t, conn)
	writeBrands(t, &svc)
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

func TestNeoService_DoNotReadBrokenConcepts(t *testing.T) {
	conn := getDatabaseConnection(t)
	svc := concepts.NewConceptService(conn)
	assert.NoError(t, svc.Initialise())

	tests := []struct {
		name                string
		conceptType         string
		conceptFixture      string
		annotationsFixture  string
		annotationsPlatform string
		brokenConceptUUID   string
	}{
		{
			name:                "Brands",
			conceptType:         "Brand",
			conceptFixture:      fmt.Sprintf("./fixtures/Brand-%s-child.json", brandChildUUID),
			annotationsFixture:  fmt.Sprintf("./fixtures/Annotations-%s.json", contentUUID),
			annotationsPlatform: "v1",
			brokenConceptUUID:   brandChildUUID,
		},
		{
			name:                "Organisations",
			conceptType:         "Organisation",
			conceptFixture:      fmt.Sprintf("./fixtures/Organisation-Fakebook-%s.json", companyUUID),
			annotationsFixture:  fmt.Sprintf("./fixtures/Annotations-%s-org.json", contentUUID),
			annotationsPlatform: "v2",
			brokenConceptUUID:   companyUUID,
		},
		{
			name:                "People",
			conceptType:         "Person",
			conceptFixture:      fmt.Sprintf("./fixtures/Person-%s.json", personUUID),
			annotationsFixture:  fmt.Sprintf("./fixtures/Annotations-%s-person.json", contentUUID),
			annotationsPlatform: "pac",
			brokenConceptUUID:   personUUID,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cleanDB(t, conn)
			writeJSONToConceptService(t, &svc, test.conceptFixture)
			writeContent(t, conn)
			writeAnnotation(t, conn, test.annotationsFixture, test.annotationsPlatform)

			// Delete canonical node so we can check that we are not returning broken concepts
			results := []Concept{}
			query := &neoism.CypherQuery{
				Statement: "MATCH (c:Concept{prefUUID:{uuid}}) DETACH DELETE c",
				Parameters: neoism.Props{
					"uuid": test.brokenConceptUUID,
				},
				Result: &results,
			}
			err := conn.CypherBatch([]*neoism.CypherQuery{query})
			if err != nil {
				t.Fatalf("Error deleting canonical node: %v", err)
			}

			neoSvc := NewNeoService(conn, "not-needed")

			conceptCh := make(chan Concept)
			count, found, err := neoSvc.Read(test.conceptType, conceptCh)

			assert.NoError(t, err, "Error reading from Neo")
			assert.False(t, found)
			assert.Equal(t, 0, count)
		})
	}
}

func TestNeoService_ReadHasBrand(t *testing.T) {
	conn := getDatabaseConnection(t)
	svc := concepts.NewConceptService(conn)
	assert.NoError(t, svc.Initialise())

	cleanDB(t, conn)
	writeBrands(t, &svc)
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
	assert.NoError(t, svc.Initialise())

	tests := []struct {
		name                 string
		fixture              string
		expectedFactsetRegex string
	}{
		{
			name:                 "Organisation with 0 Factset Sources",
			fixture:              fmt.Sprintf("./fixtures/Organisation-Fakebook-%s.json", companyUUID),
			expectedFactsetRegex: `^$`,
		},
		{
			name:                 "Organisation with 1 Factset Sources",
			fixture:              fmt.Sprintf("./fixtures/Organisation-Fakebook-%s-Factset.json", companyUUID),
			expectedFactsetRegex: `^FACTSET1$`,
		},
		{
			name:                 "Organisation with 2 Factset Sources",
			fixture:              fmt.Sprintf("./fixtures/Organisation-Fakebook-%s-Factset2.json", companyUUID),
			expectedFactsetRegex: `^FACTSET\d;FACTSET\d$`, // We cannot guarantee the order of the IDs
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cleanDB(t, conn)
			writeJSONToConceptService(t, &svc, test.fixture)
			writeJSONToConceptService(t, &svc, fmt.Sprintf("./fixtures/FinancialInstrument-%s.json", financialInstrumentUUID))

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
					assert.Regexp(t, regexp.MustCompile(test.expectedFactsetRegex), c.FactsetId)
				case <-time.After(3 * time.Second):
					t.FailNow()
				}
			}
		})
	}
}

func TestNeoService_ReadPerson(t *testing.T) {
	conn := getDatabaseConnection(t)
	svc := concepts.NewConceptService(conn)
	assert.NoError(t, svc.Initialise())

	tests := []struct {
		name               string
		uuid               string
		conceptFixture     string
		annotationsFixture string
		expectedCount      int
		expectedPrefLabel  string
		readAs             string
	}{
		{
			name:               "Standard Person",
			uuid:               personUUID,
			conceptFixture:     fmt.Sprintf("./fixtures/Person-%s.json", personUUID),
			annotationsFixture: fmt.Sprintf("./fixtures/Annotations-%s-person.json", contentUUID),
			expectedCount:      1,
			expectedPrefLabel:  "Peter Foster",
			readAs:             "Person",
		},
		{
			name:               "Person with Brand Read As Person",
			uuid:               personWithBrandUUID,
			conceptFixture:     fmt.Sprintf("./fixtures/Person-%s-With-Brand.json", personWithBrandUUID),
			annotationsFixture: fmt.Sprintf("./fixtures/Annotations-%s-person-with-brand.json", contentUUID),
			expectedCount:      1,
			expectedPrefLabel:  "Jancis Robinson",
			readAs:             "Person",
		},
		{
			name:               "Person with Brand Not Read As Brand",
			uuid:               personWithBrandUUID,
			conceptFixture:     fmt.Sprintf("./fixtures/Person-%s-With-Brand.json", personWithBrandUUID),
			annotationsFixture: fmt.Sprintf("./fixtures/Annotations-%s-person-with-brand.json", contentUUID),
			expectedCount:      0,
			readAs:             "Brand",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cleanDB(t, conn)
			writeJSONToConceptService(t, &svc, test.conceptFixture)
			writeContent(t, conn)
			writeAnnotation(t, conn, test.annotationsFixture, "pac")
			neoSvc := NewNeoService(conn, "not-needed")

			conceptCh := make(chan Concept)
			count, found, err := neoSvc.Read(test.readAs, conceptCh)

			assert.NoError(t, err, "Error reading from Neo")
			assert.Equal(t, test.expectedCount, count)
			if test.expectedCount == 0 {
				assert.False(t, found)
			} else {
				assert.True(t, found)
			waitLoop:
				for {
					select {
					case c, open := <-conceptCh:
						if !open {
							break waitLoop
						}
						assert.Equal(t, test.uuid, c.Uuid)
						assert.Equal(t, "http://api.ft.com/things/"+test.uuid, c.Id)
						assert.Equal(t, "http://api.ft.com/people/"+test.uuid, c.ApiUrl)
						assert.Equal(t, test.expectedPrefLabel, c.PrefLabel)
						assertListContainsAll(t, []string{"Thing", "Concept", "Person"}, c.Labels)
					case <-time.After(3 * time.Second):
						t.FailNow()
					}
				}
			}
		})
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

func writeBrands(t *testing.T, service concepts.ConceptServicer) {
	writeJSONToConceptService(t, service, fmt.Sprintf("./fixtures/Brand-%s-parent.json", brandParentUUID))
	writeJSONToConceptService(t, service, fmt.Sprintf("./fixtures/Brand-%s-child.json", brandChildUUID))
	writeJSONToConceptService(t, service, fmt.Sprintf("./fixtures/Brand-%s-grand_child.json", brandGrandChildUUID))
}

func writeJSONToConceptService(t *testing.T, service concepts.ConceptServicer, pathToJsonFile string) {
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
			DETACH DELETE i, t, a`, uuid),
		}
	}
	err := db.CypherBatch(qs)
	assert.NoError(t, err)
}
