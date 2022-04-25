package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"cs.utexas.edu/zjia/faas-retwis/utils"

	"cs.utexas.edu/zjia/faas/slib/statestore"
	"cs.utexas.edu/zjia/faas/types"

	"go.mongodb.org/mongo-driver/mongo"
)

type initHandler struct {
	kind   string
	env    types.Environment
	client *mongo.Client
}

func NewSlibInitHandler(env types.Environment) types.FuncHandler {
	return &initHandler{
		kind: "slib",
		env:  env,
	}
}

func NewMongoInitHandler(env types.Environment) types.FuncHandler {
	return &initHandler{
		kind:   "mongo",
		env:    env,
		client: utils.CreateMongoClientOrDie(context.TODO()),
	}
}

func initSlib(ctx context.Context, env types.Environment) error {
	store := statestore.CreateEnv(ctx, env)

	if result := store.Object("timeline").MakeArray("posts", 0); result.Err != nil {
		return result.Err
	}

	if result := store.Object("next_user_id").SetNumber("value", 0); result.Err != nil {
		return result.Err
	}

	return nil
}

func initMongo(ctx context.Context) error {
	ctx, _ = context.WithTimeout(context.Background(), 300*time.Second)  
	db, err := sql.Open("mysql", "boki:retwisboki@tcp(boki.chou4ursccnw.us-east-2.rds.amazonaws.com:3306)/retwis")
	if err != nil {
		panic(err)
	}
	query := "DROP TABLE users"
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		panic(err)
	}
	query = "DROP TABLE posts"
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		panic(err)
	}
	query = "DROP TABLE follow"
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		panic(err)
	}
	query = "CREATE TABLE IF NOT EXISTS users (user_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, username varchar(255) NOT NULL UNIQUE, password varchar(255) NOT NULL, auth varchar(255) NOT NULL, followers INT NOT NULL, followees INT NOT NULL, posts INT NOT NULL)"
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		panic(err)
	}
	fmt.Println("Created user table")
	// Create
	query = "CREATE TABLE IF NOT EXISTS posts (post_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, body varchar(255) NOT NULL, user_id BIGINT NOT NULL, username VARCHAR(255) NOT NULL)"
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		panic(err)
	}
	
	fmt.Println("Created posts table")
	query = "CREATE TABLE IF NOT EXISTS follow (user_id BIGINT NOT NULL, followee_id BIGINT NOT NULL)"
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		panic(err)
	}
	
	fmt.Println("Created follow table")
	defer db.Close()
	return nil
}

func initMongo_bkp(ctx context.Context, client *mongo.Client) error {
	db := client.Database("retwis")

	if err := utils.MongoCreateCounter(ctx, db, "next_user_id"); err != nil {
		return err
	}

	if err := utils.MongoCreateIndex(ctx, db.Collection("users"), "userId", true /* unique */); err != nil {
		return err
	}

	if err := utils.MongoCreateIndex(ctx, db.Collection("users"), "username", true /* unique */); err != nil {
		return err
	}

	return nil
}

func (h *initHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	var err error
	switch h.kind {
	case "slib":
		err = initSlib(ctx, h.env)
	case "mongo":
		//err = initSQL(ctx)
		err = initMongo(ctx)
	default:
		panic(fmt.Sprintf("Unknown kind: %s", h.kind))
	}

	if err != nil {
		return nil, err
	} else {
		return []byte("Init done\n"), nil
	}
}
