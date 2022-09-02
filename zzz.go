package main

import (
	"context"
	"database/sql"
	"flag"
	// "sync/atomic"
	"sync"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jamiealquiza/tachymeter"
	// "github.com/jamiealquiza/tachymeter"
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

	now := time.Now()
	fmt.Println("benchmark start ...")

	var wg sync.WaitGroup
	wg.Add(30)

	// for i := 0; i < 3; i++ {
	// 	go func() {
	conn, err := db.Conn(context.Background())
	assert(err)

	defer wg.Done()
	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), "use sbtest")
	assert(err)


	// stmt, err := conn.PrepareContext(context.Background(), `insert into tmp select from test where id = ?`)
	// assert(err)

	var count int64
	t := tachymeter.New(&tachymeter.Config{Size: 50})
	
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


	time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)

	for {

		start := time.Now()

		rows, err := conn.QueryContext(context.Background(), fmt.Sprintf("insert into ttt select * from sbtest%d limit 40000", rand.Intn(8) + 1))
		assert(err)
		rows.Close()


		t.AddTime(time.Since(start))


		// for x:=0; x<1000; x++ {
		// 	_, err := stmt.Exec(x)
		// 	assert(err)
		// }
		
		// start := time.Now()

		// _, err = conn.ExecContext(context.Background(), "insert into tmp select * from test")
		// assert(err)

		// dur := time.Since(start)
		// fmt.Println("finish takes ===", dur)
	}
	// }()
	// }


	wg.Wait()

	fmt.Println("benchmark end ...", time.Since(now))
}
