package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/gopher/gomagic"
	_ "github.com/lib/pq"
)

const numAnalyzers = 1
const txCommitInterval = 1000

// command line options
var dbUser string
var dbName string
var dbPass string
var dbHost string

func init() {
	flag.StringVar(&dbUser, "u", "", "database user")
	flag.StringVar(&dbName, "n", "", "database name")
	flag.StringVar(&dbPass, "p", "", "database password")
	flag.StringVar(&dbHost, "h", "", "database host")
}

// FileEntry captures the POSIX attributes for a filesystem entry (file, dir, link, ...)
type FileEntry struct {
	path string
	info os.FileInfo
}

func main() {

	flag.Parse()
	if dbUser == "" || dbName == "" || dbPass == "" || dbHost == "" {
		Usage()
	}

	if len(flag.Args()) == 0 {
		Usage()
	}
	filePath := flag.Args()[0]

	var wg sync.WaitGroup

	db, err := createTable()
	if err != nil {
		log.Fatal("Failed to create database table with ", err)
	}

	fileChannel := make(chan FileEntry)

	for i := 0; i < numAnalyzers; i++ {
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
func createTable() (*sql.DB, error) {
	authString := fmt.Sprintf("user=%s dbname=%s password=%s host=%s sslmode=disable",
		dbUser, dbName, dbPass, dbHost)
	db, err := sql.Open("postgres", authString)
	if err != nil {
		return nil, err
	}

	sqlStmt := `
	CREATE TABLE IF NOT EXISTS file_info (
		id SERIAL NOT NULL PRIMARY KEY,
		name TEXT,
		size BIGINT,
		mode BIGINT,
		time TEXT,
		extension TEXT,
		is_dir BOOLEAN,
		short_file_info TEXT,
		file_info TEXT);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return nil, err
	}
	return db, nil
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
		// we ignore eny errors (such as permission denied, etc.), log them and soldier on
		if err != nil {
			log.Println("fileTreeWalker: ", err)
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
func analyzer(files <-chan FileEntry, db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	magic, err := gomagic.New(gomagic.NoneFlag)
	if err != nil {
		return
	}

	sqlAddEntry := `
		INSERT INTO file_info (
			name,
			size,
			mode,
			time,
			extension,
			is_dir,
			short_file_info,
			file_info
		) values($1, $2, $3, $4, $5, $6, $7, $8);
		`

	txCount := 0
	var tx *sql.Tx
	var stmt *sql.Stmt
	for e := range files {

		// prepare new transaction; if it fails we bail for now
		if txCount == 0 {
			tx, err = db.Begin()
			if err != nil {
				log.Fatal("Failed to prepare transaction")
			}
			stmt, err = tx.Prepare(sqlAddEntry)
			if err != nil {
				log.Fatal("Failed to open database")
			}
		}

		filePath := path.Join(e.path, e.info.Name())
		if e.info.IsDir() {
			filePath = e.path
		}
		fileExt := path.Ext(e.info.Name())

		// strip dot from extension
		if len(fileExt) != 0 {
			fileExt = fileExt[1:]
		}

		fileInfo, err := magic.ExamineFile(filePath)
		if err != nil {
			log.Println("gomagic failed on ", filePath, " with", err)
		}

		shortFileInfo := ""
		if fileInfo != "" {
			shortFileInfo = strings.Split(fileInfo, ",")[0]
		}

		if _, err = stmt.Exec(filePath, e.info.Size(), e.info.Mode(), e.info.ModTime(), fileExt,
			e.info.IsDir(), shortFileInfo, fileInfo); err != nil {
			log.Printf("Failed to %s insert transaction into database with: %s\n", filePath, err)
		}

		txCount++
		if txCount == txCommitInterval {
			tx.Commit()
			stmt.Close()
			txCount = 0
		}
	}

	// make sure to commit the last transaction in flight
	if txCount != 0 {
		tx.Commit()
	}
}

// Usage prints the a quick info on how to use the client
func Usage() {
	fmt.Println("Usage: calxy -h <host> -n <dbname> -p <dbpass> -u <dbuser> file_tree_root")
	flag.PrintDefaults()
	os.Exit(1)
}
