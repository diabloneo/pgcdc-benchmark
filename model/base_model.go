package model

import (
	"strconv"
	"sync"
	"time"

	"gorm.io/gorm"
)

// Model defines basic columns used by all models.
// This is used to replace gorm.Model and has a copy in model/migration/version.
type Model struct {
	ID        int64          `gorm:"primarykey" json:"id"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted_at"`
}

// StringID returns string format of ID.
func (m *Model) StringID() string {
	return strconv.FormatInt(m.ID, 10)
}

var (
	// AllModels contains all models used by this application.
	AllModels         = make([]interface{}, 0, 1)
	registerModelLock = new(sync.Mutex)
)

func registerModels(m ...interface{}) {
	registerModelLock.Lock()
	defer registerModelLock.Unlock()

	AllModels = append(AllModels, m...)
}
