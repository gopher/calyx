package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/gopher/gomagic"
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

	if len(os.Args) == 1 {
		log.Fatal("usage: calyx <filepath>")
	}
	filePath := os.Args[1]

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

	if err := fileTreeWalker(filePath, fileChannel); err != nil {
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
	CREATE TABLE IF NOT EXISTS file_info (
		id INTEGER NOT NULL PRIMARY KEY,
		name TEXT,
		size INT64,
		mode INT64,
		time STRING,
		extension STRING,
		is_dir BOOL,
		short_file_info STRING,
		file_info STRING);
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
	for len(queue) != 0 {
		dir := queue[0]
		queue = queue[1:]

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

	// initialize gomagic
	magic, err := gomagic.New(gomagic.NoneFlag)
	if err != nil {
		return
	}

	sqlAddEntry := `
	INSERT OR REPLACE INTO file_info (
		name,
		size,
		mode,
		time,
		extension,
		is_dir,
		short_file_info,
		file_info
	) values(?, ?, ?, ?, ?, ?, ?, ?)
	`

	stmt, err := conn.Prepare(sqlAddEntry)
	if err != nil {
		log.Fatal("Failed to prepare transaction")
	}
	defer stmt.Close()

	for e := range files {
		filePath := path.Join(e.path, e.info.Name())
		fileExt := path.Ext(e.info.Name())
		// strip dot from extension
		if len(fileExt) != 0 {
			fileExt = fileExt[1:]
		}
		fileInfo, err := magic.ExamineFile(filePath)
		if err != nil {
			continue
		}
		shortFileInfo := ""
		if fileInfo != "" {
			shortFileInfo = strings.Split(fileInfo, ",")[0]
		}
		//fmt.Println(filePath, "  -->  ", shortFileInfo)

		_, err = stmt.Exec(filePath, e.info.Size(), e.info.Mode(), e.info.ModTime(), fileExt,
			e.info.IsDir(), shortFileInfo, fileInfo)
		if err != nil {
			log.Printf("Failed to %s insert transaction into database\n", filePath)
		}
	}
}
