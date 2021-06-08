package model

import (
	"time"
)

// Book table books.
type Book struct {
	Model
	Name        string    `json:"name"`
	Author      string    `json:"author"`
	PublishDate time.Time `json:"publish_date"`
}

// Page table pages.
type Page struct {
	Model
	BookID  int64 `json:"book_id"`
	PageNum int64 `json:"page_num"`
}

// Section table sections.
type Section struct {
	Model
	PageID     int64  `json:"page_id"`
	SectionNum int64  `json:"section_num"`
	Type       string `json:"type"`
}

// Paragraph table paragraphs.
type Paragraph struct {
	Model
	SectionID int64  `json:"section_id"`
	Text      string `json:"text"`
}

// Graph table graphs.
type Graph struct {
	Model
	SectionID int64 `json:"section_id"`

	// Data stores graph data using base64 encoding.
	Data string `json:"data"`
}

// Picture table pictures.
type Picture struct {
	Model
	SectionID int64 `json:"section_id"`

	// Data stores picture data using base64 encoding.
	Data string `json:"data"`
}

func init() {
	registerModels(new(Book), new(Page), new(Section), new(Paragraph), new(Graph), new(Picture))
}
