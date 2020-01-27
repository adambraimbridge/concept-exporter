package concept

import (
	"fmt"
	"testing"
	"time"

	"github.com/Financial-Times/concept-exporter/db"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockDbService struct {
	mock.Mock
}

func (m *mockDbService) Read(conceptType string, conceptCh chan db.Concept) (int, bool, error) {
	args := m.Called(conceptType, conceptCh)
	return args.Int(0), args.Bool(1), args.Error(2)
}

func TestNeoInquirer_InquireSuccessfully(t *testing.T) {
	log := logger.NewUPPLogger("Test", "PANIC")

	mockDb := new(mockDbService)
	inquirer := NewNeoInquirer(mockDb, log)

	cType := "Brand"
	mockDb.On("Read", cType, mock.AnythingOfType("chan db.Concept")).Return(2, true, nil)

	workers := inquirer.Inquire([]string{cType}, "tid_1234")

	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, 1, len(workers))
	assert.Equal(t, cType, workers[0].ConceptType)
	assert.Equal(t, 2, workers[0].GetCount())
	assert.Equal(t, STARTING, workers[0].Status)
	assert.Equal(t, 0, len(workers[0].Errch))
	mockDb.AssertExpectations(t)
}

func TestNeoInquirer_InquireSuccessfullyWithEmptyResult(t *testing.T) {
	log := logger.NewUPPLogger("Test", "PANIC")

	mockDb := new(mockDbService)
	inquirer := NewNeoInquirer(mockDb, log)

	cType := "Brand"
	mockDb.On("Read", cType, mock.AnythingOfType("chan db.Concept")).Return(0, false, nil)

	workers := inquirer.Inquire([]string{cType}, "tid_1234")

	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, 1, len(workers))
	assert.Equal(t, cType, workers[0].ConceptType)
	assert.Equal(t, 0, workers[0].GetCount())
	assert.Equal(t, STARTING, workers[0].Status)
	assert.Equal(t, 1, len(workers[0].Errch))
	assert.Equal(t, fmt.Sprintf("Reading %v concept type from Neo returned empty result", cType), (<-workers[0].Errch).Error())
	mockDb.AssertExpectations(t)
}

func TestNeoInquirer_InquireWithError(t *testing.T) {
	log := logger.NewUPPLogger("Test", "PANIC")

	mockDb := new(mockDbService)
	inquirer := NewNeoInquirer(mockDb, log)

	cType := "Brand"
	mockDb.On("Read", cType, mock.AnythingOfType("chan db.Concept")).Return(0, false, errors.New("Neo err"))

	workers := inquirer.Inquire([]string{cType}, "tid_1234")

	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, 1, len(workers))
	assert.Equal(t, cType, workers[0].ConceptType)
	assert.Equal(t, 0, workers[0].GetCount())
	assert.Equal(t, STARTING, workers[0].Status)
	assert.Equal(t, 1, len(workers[0].Errch))
	assert.Equal(t, "Neo err", (<-workers[0].Errch).Error())
	mockDb.AssertExpectations(t)
}
