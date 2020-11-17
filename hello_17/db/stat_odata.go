package db

import (
	"database/sql"
	"time"
)

type DBrchAssetStat struct {
	OrgId      sql.NullString
	BalDate    time.Time
	AgmtName   sql.NullString
	PaperNo    sql.NullString
	MobileNum  sql.NullString
	QryBal     string
	FixBal     string
	GfQuot     string
	FundBal    string
	InsBal     string
	MbsMobile  sql.NullString
	Servicestt sql.NullInt32
	IdHy       sql.NullString
	Phone      sql.NullString
	Hytype     sql.NullInt32
	Viplev     sql.NullString
	UpBrhCode  sql.NullString
}
