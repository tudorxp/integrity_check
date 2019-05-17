//
// This data integrity check utility was written to check that a copy/transfer of a hierarchy of files (from /path/to/DATA_OLD to /path/to/DATA_NEW) has completed successfully.
// It uses a PostgreSQL database and table to store state.
//
// There are a number of steps:
// 1. Create the table to store state, if it does not exist
// 2. If there are no entries in the table, start a transaction to add files; traverse all of the files in /path/to/DATA_NEW and add them to the table; the file walk has directory-level concurrency
// 3. For each file, compute a SHA256 hash for the /path/to/DATA_NEW version and store it in the database; this is done with a concurrency level
// 4. For each file, compute a SHA256 hash for the /path/to/DATA_OLD version and store it in the database; this is done with a concurrency level
//
// @tudorxp 2019

package main


import (
  "database/sql"
  "flag"
  "fmt"
  "encoding/json"
  "log"
  "os"
  "io"
  "sync"
  "path/filepath"
  "strings"
  // "time"
  pq "github.com/lib/pq"
  "crypto/sha256"
  // "github.com/davecgh/go-spew/spew"
)

var conf struct {
  New_path string `json:"new_path"`
  Old_path string `json:"old_path"`
  Db_connstr string `json:"db_connstr"`
  Table_name string `json:"table_name"`
  Where_clause string `json:"where_clause"`
  Db_maxconnections int `json:"db_maxconnections"`
  Db_idleconnections int `json:"db_idleconnections"`
}

var db *sql.DB
var stmt *sql.Stmt

var l = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

var wg sync.WaitGroup


func main() {

  l.Print("Starting up")

  conf_filename := flag.String("conf", "config.json", "JSON Config filename")
  flag.Parse()

  var err error
  err = load_config(conf_filename)
  die_if(err)

  // spew.Dump(conf)

  init_db()
  defer db.Close()

  // spew.Dump(db.Stats())



  // Create the state table if it does not exit
  //
  _, err = db.Exec(fmt.Sprintf(`
    create table if not exists %s (
      filename text,
      changed timestamp,
      size bigint,
      hash_new text,
      hash_old text
    )
    `,pq.QuoteIdentifier(conf.Table_name)))
  die_if(err)


  // Check the number of rows in stable

  rows := 0
  err = db.QueryRow(fmt.Sprintf("select count(*) from %s",pq.QuoteIdentifier(conf.Table_name))).Scan(&rows)
  die_if(err)

  if rows==0 { 
    l.Print("empty table, starting file walk")  

    // Walk through directory structure using a number of threads
    to_walk := make (chan string, 16)

    txn, err := db.Begin()
    die_if(err)
    stmt, err = txn.Prepare(pq.CopyIn(conf.Table_name, "filename", "size", "changed"))
    die_if(err)

    go spawn_walkers(to_walk)

    wg.Add(1)
    to_walk <- conf.New_path

    wg.Wait()

    close(to_walk)
    l.Print("walk done")

    _, err = stmt.Exec()
    die_if(err)
    err = stmt.Close()
    die_if(err)
    err = txn.Commit()
    die_if(err)
  }


  // Let's compute the new hashes

  l.Print("building hashes in path_new")

  query := fmt.Sprintf("select filename from %s where hash_new is null",conf.Table_name)
  if conf.Where_clause != "" {
    query += " and " + conf.Where_clause
  }
  l.Print("getting statement of work: ",query)

  res, err := db.Query(query)
  die_if(err)

  // spawn hashers
  hash_threads := 8
  to_hash := make (chan string, hash_threads)
  wg.Add(hash_threads)

  for i:=0; i<hash_threads; i++ {
    go hash_new_file(to_hash)
  }

  for res.Next() {
    var filename string
    err = res.Scan(&filename)
    die_if(err)
    // l.Print("sending to hash channel: ",filename)
    to_hash <- filename
  }
  
  res.Close()
  close(to_hash)
  wg.Wait()


  // And now let's compute the old hashes

  l.Print("building hashes in path_old")

  query = fmt.Sprintf("select filename from %s where hash_old is null",conf.Table_name)
  if conf.Where_clause != "" {
    query += " and " + conf.Where_clause
  }
  l.Print("getting statement of work: ",query)

  res, err = db.Query(query)
  die_if(err)

  // spawn hashers
  hash_threads = 8
  to_hash = make (chan string, hash_threads)
  wg.Add(hash_threads)

  for i:=0; i<hash_threads; i++ {
    go hash_old_file(to_hash)
  }

  for res.Next() {
    var filename string
    err = res.Scan(&filename)
    die_if(err)
    // l.Print("sending to hash channel: ",filename)
    to_hash <- filename
  }
  
  res.Close()
  close(to_hash)
  wg.Wait()

}

func load_config(config_filename *string) error {

  fd, err := os.Open(*config_filename)
  if err != nil {
    return err
  }

  // Lazily close file on any of the function exit paths
  defer fd.Close()

  // Create a new JSON decoder for the input stream - which can be anything that is capable of being read from
  js := json.NewDecoder(fd)
  if err = js.Decode(&conf); err != nil {
    return fmt.Errorf("decoding json: %s", err)
  }

  return nil
}


func die_if(err error) {
  if err != nil {
    panic(err)
  }
}


func init_db() {
  var err error
  l.Printf("got connstr: %s", conf.Db_connstr)
  db, err = sql.Open("postgres", conf.Db_connstr)
  die_if(err)
  err = db.Ping()
  die_if(err)
  db.SetMaxOpenConns(conf.Db_maxconnections)
  db.SetMaxIdleConns(conf.Db_idleconnections)
}

func spawn_walkers(to_walk chan string) {
  for p := range to_walk {
    // l.Print("spawn walker: ",p)
    go walk_dir(p,to_walk)
  } 
}

func walk_dir (dir string,to_walk chan string) {
  defer wg.Done()

  visit := func (path string, info os.FileInfo, err error) error {
    if path != dir  && err==nil && info.IsDir() {
      // l.Print("add path: ",path)
      wg.Add(1)
      to_walk <- path
      return filepath.SkipDir
    }
    if info.Mode().IsRegular() {
      _, err := stmt.Exec( strings.TrimPrefix(path,conf.New_path+"/") ,info.Size(), info.ModTime())
      die_if(err)
    }
    return nil  
  }

  filepath.Walk(dir,visit)
}




func hash_new_file (to_hash chan string) {
  defer wg.Done()

  for {
    file, ok := <- to_hash
    if !ok {
      return // channel closed
    }
    
    // l.Print("got file: ",file)
    f, err := os.Open(conf.New_path+"/"+file)
    if err != nil{
      l.Print("error opening: ",file,": ",err)
      continue
    }

    h:=sha256.New()
    if _ , err = io.Copy(h, f); err!= nil {
      l.Print("error reading from ",file,": ",err)
      f.Close()
      continue
    }
    hash:=h.Sum(nil)
    // l.Printf("hash for %s: %x",file,hash)

    // add to DB
    _, err = db.Exec( fmt.Sprintf("update %s set hash_new = $2 where filename = $1",conf.Table_name), file, fmt.Sprintf("%x",hash) )
    if err != nil {
      l.Print("error adding hash to DB: ", err)
      continue
    }

    f.Close()
  }
}


func hash_old_file (to_hash chan string) {
  defer wg.Done()

  for {
    file, ok := <- to_hash
    if !ok {
      return // channel closed
    }
    
    // l.Print("got file: ",file)
    f, err := os.Open(conf.Old_path+"/"+file)
    if err != nil{
      l.Print("error opening: ",file, ": ",err)
      continue
    }

    h:=sha256.New()
    if _ , err = io.Copy(h, f); err!= nil {
      l.Print("error reading from ",file,": ",err)
      f.Close()
      continue
    }
    hash:=h.Sum(nil)
    // l.Printf("hash for %s: %x",file,hash)

    // add to DB
    _, err = db.Exec( fmt.Sprintf("update %s set hash_old = $2 where filename = $1",conf.Table_name), file, fmt.Sprintf("%x",hash) )
    if err != nil {
      l.Print("error adding hash to DB: ", err)
      continue
    }

    f.Close()
  }
}



