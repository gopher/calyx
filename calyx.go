package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

const numAnalyzers = 4
const dbName = "file_info.db"

// FileEntry captures the POSIX attributes for a filesystem entry (file, dir, link, ...)
type FileEntry struct {
	path string
	info os.FileInfo
}

func main() {
	var wg sync.WaitGroup

	if err := createTable(); err != nil {
		log.Fatal("Failed to create database table ", dbName, " with ", err)
	}

	fileChannel := make(chan FileEntry)
	for i := 0; i < numAnalyzers; i++ {
		db, err := sql.Open("sqlite3", dbName)
		if err != nil {
			log.Fatal("Failed to open database connection to ", dbName, " with ", err)
		}
		wg.Add(1)
		go analyzer(fileChannel, db, &wg)
	}

	if err := fileTreeWalker("/Users/markus/Downloads/", fileChannel); err != nil {
		log.Fatal("file tree walk failed: ", err)
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
func fileTreeWalker(rootPath string, files chan<- FileEntry) error {
	defer close(files)

	info, err := os.Stat(rootPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("Provided root path %s is not a directory", rootPath)
	}

	queue := []string{rootPath}
	files <- FileEntry{path.Dir(rootPath), info}
	for i := 0; i < len(queue); i++ {
		dir := queue[i]
		//dir := path.Dir(elem)
		entries, err := ioutil.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if e.IsDir() {
				queue = append(queue, path.Join(dir, e.Name()))
			}
			files <- FileEntry{dir, e}
		}
	}
	return nil
}

// analyzer processes files from the files channel, analyzes them and then
// adds them to the database
func analyzer(files <-chan FileEntry, conn *sql.DB, wg *sync.WaitGroup) {
	defer conn.Close()
	defer wg.Done()
	for e := range files {
		fmt.Println(path.Join(e.path, e.info.Name()))
	}
}
