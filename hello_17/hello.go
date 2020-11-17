package main

import (
	//"strings"
	//"time"
	"strconv"
	//	_ "github.com/mattn/go-oci8"
	log "github.com/sirupsen/logrus"
	//"gopkg.in/inf.v0"
	"database/sql"
	"hello_15/db"
	//"hello_15/tasks"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	_ "github.com/lib/pq"

	"time"
)

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
	Servicestt int64
	Arg0       float64
	Arg1       float64
	IdHy       string
	Phone      string
	Hytype     int64
	Viplev     string
	UpBrhCode  string
}

func main() {

	//db, table_name := getDbTable(plat, table, date...)
	//defer db.Close()
	fmt.Println(time.Now())
	pg, err := sql.Open("postgres", "postgresql://hde:pydj!20080808@10.239.1.106/jr")
	if err != nil {
		panic(err)
	}
	//startLog(table, date...)
	//fmt.Print(pg)
	rows, err := pg.Query(`
		select org_id ,bal_date  ,agmt_name   ,paper_no   ,mobile_num   ,qry_bal  ,
		fix_bal   ,gf_quot  ,fund_bal  ,ins_bal  ,mbs_mobile ,servicestt ,
		id_hy ,phone ,hytype ,viplev ,up_brh_code     
        from ` + ` brch_asset_stat limit 10`)

	// if err != nil {
	// 	log.Fatal(err)
	// }
	defer rows.Close()
	records := make([]*CBrchAssetStat, 0)
	//i := 0
	//fmt.Println("1111")
	i := 0
	for rows.Next() {
		d := new(db.DBrchAssetStat)
		t := new(CBrchAssetStat)
		err := rows.Scan(
			&d.OrgId, &d.BalDate, &d.AgmtName, &d.PaperNo, &d.MobileNum, &d.QryBal,
			&d.FixBal, &d.GfQuot, &d.FundBal, &d.InsBal, &d.MbsMobile, &d.Servicestt,
			&d.IdHy, &d.Phone, &d.Hytype, &d.Viplev, &d.UpBrhCode)
		if err != nil {
			panic(err)
		}
		fmt.Println("OrgId====", d.OrgId, d.PaperNo)
		checkErr(err)
		if d.OrgId.Valid {

			t.OrgId = d.OrgId.String
			x := d.BalDate
			t.BalDate = time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, time.UTC)
			t.AgmtName = d.AgmtName.String
			t.PaperNo = d.PaperNo.String
			t.MobileNum = d.MobileNum.String
			k1, _ := strconv.ParseFloat(d.QryBal, 64)
			t.QryBal = k1
			k2, _ := strconv.ParseFloat(d.FixBal, 64)
			t.FixBal = k2
			k3, _ := strconv.ParseFloat(d.GfQuot, 64)
			t.GfQuot = k3
			k4, _ := strconv.ParseFloat(d.FundBal, 64)
			t.FundBal = k4
			k5, _ := strconv.ParseFloat(d.InsBal, 64)
			t.InsBal = k5
			t.MbsMobile = d.MbsMobile.String
			t.Servicestt = d.Servicestt.Int64

			t.IdHy = d.IdHy.String
			t.Phone = d.Phone.String
			t.Hytype = d.Hytype.Int64
			t.Viplev = d.Viplev.String
			t.UpBrhCode = d.UpBrhCode.String
			records = append(records, t)
			i++
		}
	}
	fmt.Println("i==============", i)
	fmt.Println("records==============", len(records))
	connect, err := sql.Open("clickhouse", "tcp://10.239.1.5:9000?username=&compress=true&debug=true&password=pydj1234")
	checkErr(err)
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Fatal(exception)
		} else {
			log.Fatal(err)
		}

	}
	tx, err := connect.Begin()
	stmt, err := tx.Prepare("INSERT INTO finance.brch_asset_stat_test1 (org_id ,bal_date   ,agmt_name  ,paper_no  ,mobile_num ,qry_bal  ,fix_bal   ,gf_quot  ,fund_bal  ,ins_bal  ,mbs_mobile ,servicestt ,id_hy ,phone ,hytype ,viplev ,up_brh_code ) VALUES (? ,?   ,?  ,?  ,? ,?  ,?   ,?  ,?  ,?  ,?,?,?,?,?,?,?  )")

	fmt.Println(stmt)

	for i, t := range records {
		if i > 0 && i%(5*10000) == 0 {
			stmt, err := tx.Prepare("INSERT INTO finance.brch_asset_stat_test1 (org_id ,bal_date   ,agmt_name  ,paper_no  ,mobile_num ,qry_bal  ,fix_bal   ,gf_quot  ,fund_bal  ,ins_bal  ,mbs_mobile ,servicestt ,id_hy ,phone ,hytype ,viplev ,up_brh_code ) VALUES (? ,?   ,?  ,?  ,? ,?  ,?   ,?  ,?  ,?  ,?,?,?,?,?,?,? ,? ,? )")
			checkErr(err)
			fmt.Println(stmt)
		}

		if _, err := stmt.Exec(
			t.OrgId, t.BalDate, t.AgmtName, t.PaperNo, t.MobileNum, t.QryBal,
			t.FixBal, t.GfQuot, t.FundBal, t.InsBal, t.MbsMobile, t.Servicestt,
			t.IdHy, t.Phone, t.Hytype, t.Viplev, t.UpBrhCode,
		); err != nil {
			log.Fatal(err)
		}

		// if _, err := stmt.Exec(
		// 	t.OrgId, t.BalDate, t.AgmtName, t.PaperNo, t.MobileNum, t.QryBal,
		// 	t.FixBal, t.GfQuot, t.FundBal, t.InsBal, t.MbsMobile, t.Servicestt,
		// 	t.Arg0, t.Arg1, t.IdHy, t.Phone, t.Hytype, t.Viplev, t.UpBrhCode,
		// ); err != nil {
		// 	log.Fatal(err)
		// }
		if i > 0 && i%(5*10000) == 0 {
			checkErr(tx.Commit())
		}
	}
	checkErr(tx.Commit())
	connect.Close()
	//fmt.Println(time.Now())
}
func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
