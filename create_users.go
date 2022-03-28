package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var FLAGS_faas_gateway string
var FLAGS_fn_prefix string
var FLAGS_num_users int
var FLAGS_followers_per_user int
var FLAGS_concurrency int
var FLAGS_rand_seed int

type user struct {
	username string
	password string
}

type follower struct {
	userId     string
	followeeId string
}

func init() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.StringVar(&FLAGS_fn_prefix, "fn_prefix", "", "")
	flag.IntVar(&FLAGS_num_users, "num_users", 1000, "")
	flag.IntVar(&FLAGS_followers_per_user, "followers_per_user", 8, "")
	flag.IntVar(&FLAGS_concurrency, "concurrency", 1, "")
	flag.IntVar(&FLAGS_rand_seed, "rand_seed", 23333, "")
	rand.Seed(int64(FLAGS_rand_seed))
}

func insertUser(db *sql.DB, p user) error {
	query := "INSERT INTO " + FLAGS_fn_prefix + "RetwisRegister(username, password) VALUES (?, ?)"
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, p.username, p.password)
	if err != nil {
		log.Printf("Error %s when inserting row into follower table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when finding rows affected", err)
		return err
	}
	log.Printf("%d followers created ", rows)
	return nil
}

func insertFolloweeId(db *sql.DB, p follower) error {
	query := "INSERT INTO " + FLAGS_fn_prefix + "RetwisFollow(username, password) VALUES (?, ?)"
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, p.userId, p.followeeId)
	if err != nil {
		log.Printf("Error %s when inserting row into user table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when finding rows affected", err)
		return err
	}
	log.Printf("%d users created ", rows)
	return nil
}

func createUsers() {
	db, err := sql.Open("mysql", "username:password@tcp(127.0.0.1:8081)/retwis")
	if err != nil {
		panic(err.Error())
	}
	_, err = db.Exec("CREATE TABLE user(username VARCHAR(20) NOT NULL, password VARCHAR(20));")
	if err != nil {
		log.Printf("create failed with error %s", err)
		return
	}
	for i := 0; i < FLAGS_num_users; i++ {
		p := user{
			username: fmt.Sprintf("testuser_%d", i),
			password: fmt.Sprintf("password_%d", i),
		}
		insertUser(db, p)
	}
	defer db.Close()
}

func createFollowers() {
	userIds1 := make([]int, 0, 1024)
	userIds2 := make([]int, 0, 1024)
	for i := 0; i < FLAGS_num_users; i++ {
		for j := 0; j < FLAGS_followers_per_user; j++ {
			followeeId := 0
			for {
				followeeId = rand.Intn(FLAGS_num_users)
				if followeeId != i {
					break
				}
			}
			userIds1 = append(userIds1, i)
			userIds2 = append(userIds2, followeeId)
		}
	}
	totalRequests := len(userIds1)
	rand.Shuffle(totalRequests, func(i, j int) {
		userIds1[i], userIds1[j] = userIds1[j], userIds1[i]
		userIds2[i], userIds2[j] = userIds2[j], userIds2[i]
	})

	db, err := sql.Open("mysql", "username:password@tcp(127.0.0.1:3306)/retwis")
	if err != nil {
		panic(err.Error())
	}
	_, err = db.Exec("CREATE TABLE user(username VARCHAR(20) NOT NULL, password VARCHAR(20));")
	if err != nil {
		log.Printf("create failed with error %s", err)
		return
	}
	for i := 0; i < totalRequests; i++ {
		p := follower{
			userId:     fmt.Sprintf("%08x", userIds1[i]),
			followeeId: fmt.Sprintf("%08x", userIds2[i]),
		}
		insertFolloweeId(db, p)
	}
}
