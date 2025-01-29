package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/squealx/datatypes"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

type Pipeline struct {
	PipelineID int64                  `json:"pipeline_id"`
	Name       string                 `json:"name"`
	Key        string                 `json:"key"`
	Metadata   datatypes.NullJSONText `json:"metadata"`
	Status     string                 `json:"status"`
	IsActive   bool                   `json:"is_active"`
	CreatedAt  time.Time              `json:"created_at"`
	UpdatedAt  time.Time              `json:"updated_at"`
	DeletedAt  datatypes.NullTime     `json:"deleted_at"`
}

func (p *Pipeline) TableName() string {
	return "pipelines"
}

func (p *Pipeline) PrimaryKey() string {
	return "pipeline_id"
}

func (p *Pipeline) ID() string {
	return fmt.Sprintf("%v", p.PipelineID)
}

func (p *Pipeline) BeforeCreate(tx *squealx.DB) error {
	p.Status = "INACTIVE"
	return nil
}

func main() {
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=oark_manager sslmode=disable", "test")
	if err != nil {
		panic(err)
	}
	pipeline := Pipeline{
		Name:     "test1",
		Key:      "test",
		Metadata: datatypes.NullJSONText{JSONText: []byte("{}"), Valid: true},
	}
	repo := squealx.New[map[string]any](db, "pipelines", "pipeline_id")
	err = repo.Update(context.Background(), &pipeline, map[string]any{
		"pipeline_id": 1,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(pipeline)
}
