package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
	"log"
	"strconv"

	_ "github.com/go-sql-driver/mysql"

	"cs.utexas.edu/zjia/faas-retwis/utils"

	"cs.utexas.edu/zjia/faas/slib/statestore"
	"cs.utexas.edu/zjia/faas/types"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type LoginInput struct {
	UserName string `json:"username"`
	Password string `json:"password"`
}

type LoginOutput struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	UserId  string `json:"userId"`
	Auth    string `json:"auth"`
}

type loginHandler struct {
	kind   string
	env    types.Environment
	client *mongo.Client
}

func NewSlibLoginHandler(env types.Environment) types.FuncHandler {
	return &loginHandler{
		kind: "slib",
		env:  env,
	}
}

func NewMongoLoginHandler(env types.Environment) types.FuncHandler {
	return &loginHandler{
		kind:   "mongo",
		env:    env,
		client: utils.CreateMongoClientOrDie(context.TODO()),
	}
}

func loginSlib(ctx context.Context, env types.Environment, input *LoginInput) (*LoginOutput, error) {
	txn, err := statestore.CreateReadOnlyTxnEnv(ctx, env)
	if err != nil {
		return nil, err
	}

	userId := ""

	userNameObj := txn.Object(fmt.Sprintf("username:%s", input.UserName))
	if value, _ := userNameObj.Get("id"); !value.IsNull() {
		userId = value.AsString()
	} else {
		return &LoginOutput{
			Success: false,
			Message: fmt.Sprintf("User name \"%s\" does not exists", input.UserName),
		}, nil
	}

	userObj := txn.Object(fmt.Sprintf("userid:%s", userId))
	if value, _ := userObj.Get("password"); !value.IsNull() {
		if input.Password != value.AsString() {
			return &LoginOutput{
				Success: false,
				Message: "Incorrect password",
			}, nil
		}
	} else {
		return &LoginOutput{
			Success: false,
			Message: fmt.Sprintf("Cannot find user with ID %s", userId),
		}, nil
	}

	output := &LoginOutput{Success: true, UserId: userId}
	if value, _ := userObj.Get("auth"); !value.IsNull() {
		output.Auth = value.AsString()
	}
	return output, nil
}

func loginMongo(ctx context.Context, input *LoginInput) (*LoginOutput, error) {
	db, err := sql.Open("mysql", "boki:retwisboki@tcp(boki.chou4ursccnw.us-east-2.rds.amazonaws.com:3306)/retwis")
	ctx, _ = context.WithTimeout(context.Background(), 300*time.Second)  
	if err != nil {
		panic(err)
	} else if err = db.Ping(); err != nil {
		panic(err)
	}
	query := "SELECT user_id, auth FROM users WHERE username = ? AND password = ?"
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return nil, err
	}
	defer stmt.Close()
	var auth string
	var user_id int64
	var username string
	var password string
	username = input.UserName
	password = input.Password
	row := stmt.QueryRowContext(ctx, username, password)
	if err := row.Scan(&auth); err != nil {
		return &LoginOutput{
			Success: false,
			Message: "Incorrect password or username",
		}, nil
	}
	str_user_id := strconv.FormatInt(user_id, 10)
	return &LoginOutput{
		Success: true,
		UserId:  str_user_id,
		Auth:    auth,
	}, nil
}

func loginMongo_bkp(ctx context.Context, client *mongo.Client, input *LoginInput) (*LoginOutput, error) {
	db := client.Database("retwis")

	var user bson.M
	if err := db.Collection("users").FindOne(ctx, bson.D{{"username", input.UserName}}).Decode(&user); err != nil {
		return &LoginOutput{
			Success: false,
			Message: fmt.Sprintf("Mongo failed: %v", err),
		}, nil
	}

	if input.Password != user["password"].(string) {
		return &LoginOutput{
			Success: false,
			Message: "Incorrect password",
		}, nil
	}
	fmt.Println("logged in")
	return &LoginOutput{
		Success: true,
		UserId:  user["userId"].(string),
		Auth:    user["auth"].(string),
	}, nil
}

func (h *loginHandler) onRequest(ctx context.Context, input *LoginInput) (*LoginOutput, error) {
	switch h.kind {
	case "slib":
		return loginSlib(ctx, h.env, input)
	case "mongo":
		//return loginSQL(ctx, input)
		return loginMongo(ctx, input)
	default:
		panic(fmt.Sprintf("Unknown kind: %s", h.kind))
	}
}

func (h *loginHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &LoginInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output, err := h.onRequest(ctx, parsedInput)
	if err != nil {
		return nil, err
	}
	return json.Marshal(output)
}
