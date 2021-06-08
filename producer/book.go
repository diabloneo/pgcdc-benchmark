package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"gorm.io/gorm"

	"pgcdc-benchmark/common"
	"pgcdc-benchmark/model"
)

func generateSection(db *model.DB, pageID, secNum int64, secType string) (err error) {
	section := &model.Section{
		PageID:     pageID,
		SectionNum: secNum,
		Type:       secType,
	}
	if err = db.Create(section).Error; err != nil {
		return errors.WithStack(err)
	}

	data, err := common.RandomBytes(16)
	if err != nil {
		return errors.WithStack(err)
	}
	buf := make([]byte, base64.StdEncoding.EncodedLen(16))
	base64.StdEncoding.Encode(buf, data)
	switch section.Type {
	case model.SectionTypeParagraph:
		para := &model.Paragraph{
			SectionID: section.ID,
			Text:      string(buf),
		}
		if err = db.Create(para).Error; err != nil {
			return errors.WithStack(err)
		}
	case model.SectionTypeGraph:
		graph := &model.Graph{
			SectionID: section.ID,
			Data:      string(buf),
		}
		if err = db.Create(graph).Error; err != nil {
			return errors.WithStack(err)
		}
	case model.SectionTypePicture:
		picture := &model.Picture{
			SectionID: section.ID,
			Data:      string(buf),
		}
		if err = db.Create(picture).Error; err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func generatePage(db *model.DB, bookID, pageNum int64, paraNum, graphNum, picNum int) (err error) {
	err = db.Transaction(func(tx *gorm.DB) (e error) {
		page := &model.Page{
			BookID:  bookID,
			PageNum: pageNum,
		}
		if e = tx.Create(page).Error; e != nil {
			return errors.WithStack(e)
		}

		var sectionNum int64 = 1
		for i := 0; i < paraNum; i++ {
			e = generateSection(model.WithDB(tx), page.ID, sectionNum, model.SectionTypeParagraph)
			if e != nil {
				return errors.WithStack(e)
			}
			sectionNum++
		}
		for i := 0; i < graphNum; i++ {
			e = generateSection(model.WithDB(tx), page.ID, sectionNum, model.SectionTypeGraph)
			if e != nil {
				return errors.WithStack(e)
			}
			sectionNum++
		}
		for i := 0; i < picNum; i++ {
			e = generateSection(model.WithDB(tx), page.ID, sectionNum, model.SectionTypePicture)
			if e != nil {
				return errors.WithStack(e)
			}
			sectionNum++
		}

		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func generateBook(prefix string) (err error) {
	db := model.NewDB(envID)

	book := &model.Book{
		Name:        fmt.Sprintf("%s-%d", prefix, rand.Int31()),
		Author:      fmt.Sprintf("author-%d", rand.Int31()),
		PublishDate: time.Now(),
	}
	log.Printf("Create book %s", book.Name)
	if err = db.Create(book).Error; err != nil {
		return errors.Wrapf(err, "create book %s", book.Name)
	}

	for i := 1; i < 100001; i++ {
		if err = generatePage(db, book.ID, int64(i), 3, 1, 1); err != nil {
			return errors.Wrapf(err, "create page %d of book %d", i, book.ID)
		}
		if i%100 == 0 {
			log.Printf("Book %s has %d pages created", book.Name, i)
		}
	}

	return nil
}
