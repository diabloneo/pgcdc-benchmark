module pgcdc-benchmark

go 1.16

require (
	github.com/jackc/pgconn v1.8.0
	github.com/jackc/pglogrepl v0.0.0-20210514235833-4afe73f2b337 // indirect
	github.com/jackc/pgproto3/v2 v2.0.6
	github.com/pkg/errors v0.9.1

	gorm.io/driver/postgres v1.0.8
	// gorm v2 starts from v1.20.0
	gorm.io/gorm v1.21.2
)
