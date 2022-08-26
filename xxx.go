package main

import (
	"context"
	"database/sql"
	"flag"
	"sync/atomic"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jamiealquiza/tachymeter"
)

var (
	host     = flag.String("h", "127.0.0.1", "host")
	port     = flag.Int("P", 4000, "port")
	user     = flag.String("u", "root", "username")
	password = flag.String("p", "", "password")
)

func varchar(size int) []byte {
	const str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	res := make([]byte, 0, size)
	for i := 0; i < size; i++ {
		res = append(res, str[rand.Intn(len(str))])
	}
	return res
}

func bigint(from, to int) int {
	return from + rand.Intn(to-from)
}

func openDB() (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/test", *user, *password, *host, *port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func mustExec(db *sql.DB, query string, args ...interface{}) {
	_, err := db.Exec(query, args...)
	if err != nil {
		log.Fatal(err)
	}
}

func assert(err error) {
	if err != nil {
		// log.Fatal(err)
		panic(err)
	}
}	

func main() {
	db, err := openDB()
	if err != nil {
		log.Fatal(err)
	}

	// mustExec(db, `use test`)

	// Create Table
	// 	mustExec(db, `CREATE TABLE test (
	// actualid bigint(20) DEFAULT NULL,
	// topid bigint(20) DEFAULT NULL,
	// planno varchar(64) DEFAULT NULL,
	// c2 varchar(64) DEFAULT NULL,
	// c3 varchar(64) DEFAULT NULL,
	// pad1 varchar(64) DEFAULT NULL,
	// pad2 varchar(64) DEFAULT NULL,
	// pad3 varchar(64) DEFAULT NULL,
	// primary key (actualid))`)

	// 	mustExec(db, `set @@tidb_enable_global_temporary_table = 1`)
	// 	Create temporary table.
	// 	mustExec(db, `CREATE GLOBAL TEMPORARY TABLE tmp (
	// actualid bigint(20) DEFAULT NULL,
	// topid bigint(20) DEFAULT NULL,
	// planno varchar(64) DEFAULT NULL,
	// c2 varchar(64) DEFAULT NULL,
	// c3 varchar(64) DEFAULT NULL,
	// pad1 varchar(64) DEFAULT NULL,
	// pad2 varchar(64) DEFAULT NULL,
	// pad3 varchar(64) DEFAULT NULL,
	// KEY topid (topid)
	// ) ON COMMIT DELETE ROWS`)

	// const md5 = "42fc97fcbf28d277721e32b9ced8f2ca"
	// const ROWS = 1000
	// Create Data
	// for i := 0; i < ROWS; i++ {
	// 	mustExec(db, "INSERT INTO test VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
	// 		i,
	// 		// bigint(10000, 20000),
	// 		bigint(20000, 30000),
	// 		varchar(50),
	// 		varchar(50),
	// 		md5,
	// 		md5,
	// 		md5,
	// 		md5)
	// }

	// var wg sync.WaitGroup
	// wg.Add(10)

	now := time.Now()
	fmt.Println("benchmark start ...")

	// 10 sessions run "insert in to select" concurrently.
	// for i := 0; i < 100; i++ {
	// go func() {
	// 	conn, err := db.Conn(context.Background())
	// 	assert(err)

	// 	defer wg.Done()
	// 	defer conn.Close()

	// 	_, err = conn.ExecContext(context.Background(), "use test")
	// 	assert(err)


	// 	_, err = conn.ExecContext(context.Background(), `set @@tmp_table_size = 1000000000`)
	// 	assert(err)

	// 	stmt, err := conn.PrepareContext(context.Background(), `insert into tmp select from test where id = ?`)
	// 	assert(err)


	// 	time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)

	// 	for {

	// 		for x:=0; x<1000; x++ {
	// 			_, err := stmt.Exec(x)
	// 			assert(err)
	// 		}
	
	// 		// start := time.Now()

	// 		// _, err = conn.ExecContext(context.Background(), "insert into tmp select * from test")
	// 		// assert(err)

	// 		// dur := time.Since(start)
	// 		// fmt.Println("finish takes ===", dur)
	// 	}
	// }()
	// }

	// wg.Wait()





	// time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)

	var count int64

	t := tachymeter.New(&tachymeter.Config{Size: 300})

	go func() {
		tick := time.NewTicker(5*time.Second)
		for range tick.C {
			total := atomic.LoadInt64(&count)
			fmt.Println("qps ==", total / 5)
			atomic.StoreInt64(&count, 0)

			fmt.Println(t.Calc())
			fmt.Println()
		}
	}()

	for i:=0; i<30; i++ {
		go func() {

			conn, err := db.Conn(context.Background())
			assert(err)
			defer conn.Close()

			_, err = conn.ExecContext(context.Background(), "use sbtest")
			assert(err)

			stmt, err := conn.PrepareContext(context.Background(), `select * from sbtest5 where id > ? and id < ?`)
			assert(err)

			for {
				// 0~500000
				v := rand.Intn(500000)

				start := time.Now()

				rows, err := stmt.Query(v, v+3)
				assert(err)
				rows.Close()

				t.AddTime(time.Since(start))

				// start := time.Now()

				// _, err = conn.ExecContext(context.Background(), "insert into tmp select * from test")
				// assert(err)

				// dur := time.Since(start)
				// fmt.Println("finish takes ===", dur)
				atomic.AddInt64(&count, 1)
			}
		}()
	}

	time.Sleep(time.Hour)

	fmt.Println("benchmark end ...", time.Since(now))
}
