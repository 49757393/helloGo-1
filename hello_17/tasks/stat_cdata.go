package tasks

import (
	//"database/sql"
	//"gopkg.in/inf.v0"
	//"github.com/shopspring/decimal"
	"time"
)

/*

 */

type CBrchAssetStat struct {
	OrgId      string
	BalDate    time.Time
	AgmtName   string
	PaperNo    string
	MobileNum  string
	QryBal     float64
	FixBal     float64
	GfQuot     float64
	FundBal    float64
	InsBal     float64
	MbsMobile  string
	Servicestt int32
	IdHy       string
	Phone      string
	Hytype     int32
	Viplev     string
	UpBrhCode  string
}
