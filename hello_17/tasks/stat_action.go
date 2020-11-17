package tasks

import (
	//"strings"
	//"time"
	//"strconv"
	//	_ "github.com/mattn/go-oci8"
	log "github.com/sirupsen/logrus"
	//"gopkg.in/inf.v0"
	"database/sql"
	//"hello_15/db"
	//"hello_15/tasks"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	_ "github.com/lib/pq"
	//"reflect"
	"time"
)

func writeClickHouseDB(data interface{}) bool {

	//h01 余额帐户日统计插入 lhq
	if brchassetstats, ok := data.([]*CBrchAssetStat); ok {

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
		stmt, err := tx.Prepare("INSERT INTO finance.brch_asset_stat_test4 (org_id ,bal_date   ,agmt_name  ,paper_no  ,mobile_num ,qry_bal  ,fix_bal   ,gf_quot  ,fund_bal  ,ins_bal  ,mbs_mobile ,servicestt ,id_hy ,phone ,hytype ,viplev ,up_brh_code,oper_time ) VALUES (?,? ,?   ,?  ,?  ,? ,?  ,?   ,?  ,?  ,?  ,?,?,?,?,?,?,? )")
		//fmt.Println("brchassetstats.len===", len(brchassetstats))
		for i, t := range brchassetstats {

			if i > 0 && i%(5*10000) == 0 {
				stmt, err := tx.Prepare("INSERT INTO finance.brch_asset_stat_test4 (org_id ,bal_date   ,agmt_name  ,paper_no  ,mobile_num ,qry_bal  ,fix_bal   ,gf_quot  ,fund_bal  ,ins_bal  ,mbs_mobile ,servicestt ,id_hy ,phone ,hytype ,viplev ,up_brh_code,oper_time ) VALUES (?,? ,?   ,?  ,?  ,? ,?  ,?   ,?  ,?  ,?  ,?,?,?,?,?,?,?  )")
				checkErr(err)
				fmt.Println(stmt)
			}
			//fmt.Println("reflect.TypeOf(t.QryBal)====", reflect.TypeOf(t.QryBal), reflect.ValueOf(t.QryBal))
			if _, err := stmt.Exec(
				t.OrgId, t.BalDate, t.AgmtName, t.PaperNo, t.MobileNum, t.QryBal,
				t.FixBal, t.GfQuot, t.FundBal, t.InsBal, t.MbsMobile, t.Servicestt,
				t.IdHy, t.Phone, t.Hytype, t.Viplev, t.UpBrhCode, time.Now(),
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
			if err != nil {
				log.Fatal(err)
				//fmt.Println(t.PaperNo)
			}
			//fmt.Println("i========", i)
		}
		checkErr(tx.Commit())
		connect.Close()
	}
	return true
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
