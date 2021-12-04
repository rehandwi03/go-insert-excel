package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/xuri/excelize/v2"
	"log"
	"math/rand"
	"sync"
	"time"
)

type M map[string]interface{}

type User struct {
	NIK string `json:"nik"`
	Name string `json:"name"`
	Position string `json:"position"`
}

func connectDB() *sql.DB {
	host := "127.0.0.1"
	port := "5432"
	user := "rehan123"
	password := "rehan123"
	dbname := "rehan123"

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbname)
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}

	conn.SetMaxOpenConns(30)

	return conn
}

var conn *sql.DB

func main() {
	now := time.Now()
	log.Printf("start:  %v", now)

	log.SetFlags(log.Lshortfile | log.Lmicroseconds)

	conn = connectDB()

	datas := getRowData()

	// withOutGoroutine(datas)

	withGoroutine(datas)


	log.Printf("end: %v", time.Since(now))
}

func withOutGoroutine(datas []User)  {
	for _, data := range datas {
		insert(data)
	}
}

func worker(jobs <-chan User, wg *sync.WaitGroup) {
	for j := range jobs {
		// fmt.Printf("worker %d processing job %d\n", id, j)
		insert(j)
		// fmt.Printf("worker %d finish job %d\n", id, j)
		// time.Sleep(time.Second)
		wg.Done()
	}
}

func withGoroutine(datas []User)  {
	jobs := make(chan User, len(datas))
	wg := sync.WaitGroup{}

	for w := 1; w <= 100; w++ {
		go worker(jobs, &wg)
	}

	for _, data := range datas {
		jobs <- data
		wg.Add(1)
	}

	close(jobs)
	wg.Wait()

	// for a := 1; a <= 9; a++ {
	// 	<-results
	// }
}



func insert(data User)  {
	stmt, err := conn.Prepare("INSERT INTO users(id, nik, emp_name, emp_position) VALUES($1, $2, $3, $4)")
	if err != nil {
		log.Fatalf("stmt error: %v", err)
	}

	_, err = stmt.Exec(rand.Int(), data.NIK, data.Name, data.Position)
	if err != nil {
		log.Fatalf("exec error: %v", err)
	}

}

func getRowData() []User {
	f, err := excelize.OpenFile("./excel.xlsx")
	if err != nil {
		log.Fatal(err)
	}

	sheetName := "Sheet1"

	rows, err := f.GetRows(sheetName)
	if err != nil {
		log.Fatal(err)
	}


	mapData := make([]M, 0)
	//
	for i, _ := range rows {
		i++
		name, err := f.GetCellValue(sheetName, fmt.Sprintf("B%d", i))
		if err != nil {
			log.Fatalf("name: %v", err)
		}

		position, err := f.GetCellValue(sheetName, fmt.Sprintf("C%d", i))
		if err != nil {
			log.Fatalf("name: %v", err)
		}

		nik, err := f.GetCellValue(sheetName, fmt.Sprintf("D%d", i))
		if err != nil {
			log.Fatalf("name: %v", err)
		}

		data  := M{
			"nik": nik,
			"name": name,
			"position":position,
		}

		mapData = append(mapData, data)
	}

	jsonBody, err := json.Marshal(mapData)
	if err != nil {
		log.Fatalf("error marshal json: %v", err)
	}

	response := make([]User, 0)

	err = json.Unmarshal(jsonBody, &response)
	if err != nil {
		log.Fatalf("error unmarshaling: %v", err)
	}

	return response
}
