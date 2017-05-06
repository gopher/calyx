package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

const numAnalyzers = 4
const dbName = "file_info.db"

func main() {
	var wg sync.WaitGroup

	if err := createTable(); err != nil {
		log.Fatal("Failed to create database table ", dbName, " with ", err)
	}

	done := make(chan interface{})
	fileChannel := make(chan string)
	for i := 0; i < numAnalyzers; i++ {
		db, err := sql.Open("sqlite3", dbName)
		if err != nil {
			log.Fatal("Failed to open database connection to ", dbName, " with ", err)
		}
		wg.Add(1)
		go processor(fileChannel, db, done, &wg)
	}
	wg.Wait()
}

// createTable creates the initial database table used for storing the
// information of the file tree walk
func createTable() error {
	// create the initial database table
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		return err
	}

	sqlStmt := `
	create table file_info (id integer not null primary key, name text, size int64, mode int64, time string, is_dir bool);
	delete from file_info;
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return err
	}
	return nil
}

// fileTreeWalker walks the POSIX file tree under root and sends the
// paths to all file objects underneath to the file channel
// TODO: This version should be replaced by a multithreaded version
// for efficiency
func fileTreeWalker(root string, files chan<- string) {

}

// analyzer processes files from the files channel, analyzes them and then
// adds them to the database
func processor(files <-chan string, conn *sql.DB, done chan interface{}, wg *sync.WaitGroup) {
	defer conn.Close()
	defer wg.Done()
	fmt.Println("done processing")
}
