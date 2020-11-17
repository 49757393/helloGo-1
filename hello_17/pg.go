package main

import (
	//"strings"
	//"time"
	"strconv"
	//	_ "github.com/mattn/go-oci8"
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	//"gopkg.in/inf.v0"
	//"github.com/ClickHouse/clickhouse-go"
	_ "github.com/lib/pq"
	"hello_15/db"
	"hello_15/tasks"
	//"math"
	//"reflect"
	//"sync"
	//"github.com/shopspring/decimal"
	"time"
)

func main() {

	fmt.Println(time.Now())
	pg, err := sql.Open("postgres", "postgresql://hde:pydj!20080808@10.239.1.106/jr")
	if err != nil {
		panic(err)
	}

	rows, err := pg.Query(`
		select org_id ,bal_date  ,agmt_name   ,paper_no   ,mobile_num   ,qry_bal  ,
		fix_bal   ,gf_quot  ,fund_bal  ,ins_bal  ,mbs_mobile ,servicestt ,
		id_hy ,phone ,hytype ,viplev ,up_brh_code     
        from ` + ` brch_asset_stat `)

	// if err != nil {
	// 	log.Fatal(err)
	// }
	defer rows.Close()
	records := make([]*tasks.CBrchAssetStat, 0)
	i := 0

	for rows.Next() {
		d := new(db.DBrchAssetStat)
		t := new(tasks.CBrchAssetStat)
		err := rows.Scan(
			&d.OrgId, &d.BalDate, &d.AgmtName, &d.PaperNo, &d.MobileNum, &d.QryBal,
			&d.FixBal, &d.GfQuot, &d.FundBal, &d.InsBal, &d.MbsMobile, &d.Servicestt,
			&d.IdHy, &d.Phone, &d.Hytype, &d.Viplev, &d.UpBrhCode)
		if err != nil {
			panic(err)
		}

		checkErr(err)

		if i > 0 && i%(5*10000) == 0 {

			tasks.InitChan()
			tasks.Start(records)
			records = make([]*tasks.CBrchAssetStat, 0)

		}

		t.OrgId = d.OrgId.String
		x := d.BalDate
		t.BalDate = time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, time.UTC)
		t.AgmtName = d.AgmtName.String
		t.PaperNo = d.PaperNo.String
		t.MobileNum = d.MobileNum.String

		// k1, _ := strconv.ParseFloat(d.QryBal, 64)
		// t.QryBal = k1
		// k2, _ := strconv.ParseFloat(d.FixBal, 64)
		// t.FixBal = k2
		// k3, _ := strconv.ParseFloat(d.GfQuot, 64)
		// t.GfQuot = k3
		// k4, _ := strconv.ParseFloat(d.FundBal, 64)
		// t.FundBal = k4
		// k5, _ := strconv.ParseFloat(d.InsBal, 64)
		// t.InsBal = k5
		//y := new(inf.Dec)
		//转换成inf.Dec精度数字
		//y.SetString(d.QryBal)

		k1, _ := strconv.ParseFloat(d.QryBal, 64)
		//fmt.Println("t.QryBal=========", decimal.NewFromFloat(k1))
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
		t.Servicestt = d.Servicestt.Int32

		t.IdHy = d.IdHy.String
		t.Phone = d.Phone.String
		t.Hytype = d.Hytype.Int32
		t.Viplev = d.Viplev.String
		t.UpBrhCode = d.UpBrhCode.String

		records = append(records, t)
		i = i + 1

	}
	tasks.InitChan()
	tasks.Start(records)
	fmt.Println(time.Now())
}
func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
