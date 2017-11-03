package concept

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockHttpClient struct {
	mock.Mock
}

func (c *mockHttpClient) Do(req *http.Request) (resp *http.Response, err error) {
	args := c.Called(req)
	return args.Get(0).(*http.Response), args.Error(1)
}

type mockS3WriterServer struct {
	mock.Mock
}

func (m *mockS3WriterServer) startMockS3WriterServer(t *testing.T) *httptest.Server {
	router := mux.NewRouter()
	router.HandleFunc("/concept/{fileName}", func(w http.ResponseWriter, r *http.Request) {
		ua := r.Header.Get("User-Agent")
		assert.Equal(t, "UPP Concept Exporter", ua, "user-agent header")

		contentTypeHeader := r.Header.Get("Content-Type")
		tid := r.Header.Get("X-Request-Id")

		fileName, ok := mux.Vars(r)["fileName"]
		assert.NotNil(t, fileName)
		assert.True(t, ok)

		body, err := ioutil.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.True(t, len(body) > 0)

		w.WriteHeader(m.UploadRequest(fileName, tid, contentTypeHeader))

	}).Methods(http.MethodPut)

	router.HandleFunc("/__gtg", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(m.GTG())
	}).Methods(http.MethodGet)

	return httptest.NewServer(router)
}

func (m *mockS3WriterServer) GTG() int {
	args := m.Called()
	return args.Int(0)
}

func (m *mockS3WriterServer) UploadRequest(fileName, tid, contentTypeHeader string) int {
	args := m.Called(fileName, tid, contentTypeHeader)
	return args.Int(0)
}

func TestS3UpdaterUploadConcept(t *testing.T) {
	testConcept := "Brand"

	mockServer := new(mockS3WriterServer)
	mockServer.On("UploadRequest", testConcept + ".csv", "tid_1234", "application/json").Return(200)
	server := mockServer.startMockS3WriterServer(t)

	updater := NewS3Updater(server.URL)

	err := updater.Upload([]byte("test"), testConcept + ".csv", "tid_1234")
	assert.NoError(t, err)
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterUploadContentErrorResponse(t *testing.T) {
	testConcept := "Brand"

	mockServer := new(mockS3WriterServer)
	mockServer.On("UploadRequest", testConcept + ".csv", "tid_1234", "application/json").Return(503)
	server := mockServer.startMockS3WriterServer(t)

	updater := NewS3Updater(server.URL)

	err := updater.Upload([]byte("test"), testConcept + ".csv", "tid_1234")
	assert.Error(t, err)
	assert.Equal(t, "UPP Export RW S3 returned HTTP 503", err.Error())
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterUploadContentWithErrorOnNewRequest(t *testing.T) {
	updater := NewS3Updater("://")

	err := updater.Upload([]byte("test"), "Brand.csv", "tid_1234")
	assert.Error(t, err)
	assert.Equal(t, "parse :///concept/Brand.csv: missing protocol scheme", err.Error())
}

func TestS3UpdaterUploadContentErrorOnRequestDo(t *testing.T) {
	mockClient := new(mockHttpClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(&http.Response{}, errors.New("Http Client err"))

	updater := &S3Updater{Client: mockClient,
		S3WriterBaseURL: "http://server",
	}

	err := updater.Upload([]byte("test"), "Brand.csv", "tid_1234")
	assert.Error(t, err)
	assert.Equal(t, "Http Client err", err.Error())
	mockClient.AssertExpectations(t)
}

func TestS3UpdaterCheckHealth(t *testing.T) {
	mockServer := new(mockS3WriterServer)
	mockServer.On("GTG").Return(200)
	server := mockServer.startMockS3WriterServer(t)

	updater := NewS3Updater(server.URL)

	resp, err := updater.(*S3Updater).CheckHealth()
	assert.NoError(t, err)
	assert.Equal(t, "S3 Writer is good to go.", resp)
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterCheckHealthError(t *testing.T) {
	mockServer := new(mockS3WriterServer)
	mockServer.On("GTG").Return(503)
	server := mockServer.startMockS3WriterServer(t)

	updater := NewS3Updater(server.URL)

	resp, err := updater.(*S3Updater).CheckHealth()
	assert.Error(t, err)
	assert.Equal(t, "S3 Writer is not good to go.", resp)
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterCheckHealthErrorOnNewRequest(t *testing.T) {
	updater := &S3Updater{Client: &http.Client{},
		S3WriterHealthURL: "://",
	}

	resp, err := updater.CheckHealth()
	assert.Error(t, err)
	assert.Equal(t, "parse ://: missing protocol scheme", err.Error())
	assert.Equal(t, "Error in building request to check if the S3 Writer is good to go", resp)
}

func TestS3UpdaterCheckHealthErrorOnRequestDo(t *testing.T) {
	mockClient := new(mockHttpClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(&http.Response{}, errors.New("Http Client err"))

	updater := &S3Updater{Client: mockClient,
		S3WriterBaseURL:   "http://server",
		S3WriterHealthURL: "http://server",
	}

	resp, err := updater.CheckHealth()
	assert.Error(t, err)
	assert.Equal(t, "Http Client err", err.Error())
	assert.Equal(t, "Error in getting request to check if S3 Writer is good to go.", resp)
	mockClient.AssertExpectations(t)
}

func NewS3Updater(s3WriterBaseURL string) Updater {
	return &S3Updater{Client: &http.Client{},
		S3WriterBaseURL:   s3WriterBaseURL,
		S3WriterHealthURL: s3WriterBaseURL + "/__gtg",
	}
}
