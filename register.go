package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"

	"cs.utexas.edu/zjia/faas-retwis/utils"

	"cs.utexas.edu/zjia/faas/slib/statestore"
	"cs.utexas.edu/zjia/faas/types"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type RegisterInput struct {
	UserName string `json:"username"`
	Password string `json:"password"`
}

type RegisterOutput struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	UserId  string `json:"userId"`
}

type registerHandler struct {
	kind   string
	env    types.Environment
	client *mongo.Client
}

func NewSlibRegisterHandler(env types.Environment) types.FuncHandler {
	return &registerHandler{
		kind: "slib",
		env:  env,
	}
}

func NewMongoRegisterHandler(env types.Environment) types.FuncHandler {
	return &registerHandler{
		kind:   "mongo",
		env:    env,
		client: utils.CreateMongoClientOrDie(context.TODO()),
	}
}

func registerSlib(ctx context.Context, env types.Environment, input *RegisterInput) (*RegisterOutput, error) {
	store := statestore.CreateEnv(ctx, env)
	nextUserIdObj := store.Object("next_user_id")
	result := nextUserIdObj.NumberFetchAdd("value", 1)
	if result.Err != nil {
		return nil, result.Err
	}
	userIdValue := uint32(result.Value.AsNumber())

	txn, err := statestore.CreateTxnEnv(ctx, env)
	if err != nil {
		return nil, err
	}

	userNameObj := txn.Object(fmt.Sprintf("username:%s", input.UserName))
	if value, _ := userNameObj.Get("id"); !value.IsNull() {
		txn.TxnAbort()
		return &RegisterOutput{
			Success: false,
			Message: fmt.Sprintf("User name \"%s\" already exists", input.UserName),
		}, nil
	}

	userId := fmt.Sprintf("%08x", userIdValue)
	userNameObj.SetString("id", userId)

	userObj := txn.Object(fmt.Sprintf("userid:%s", userId))
	userObj.SetString("username", input.UserName)
	userObj.SetString("password", input.Password)
	userObj.SetString("auth", fmt.Sprintf("%016x", rand.Uint64()))
	userObj.MakeObject("followers")
	userObj.MakeObject("followees")
	userObj.MakeArray("posts", 0)

	if committed, err := txn.TxnCommit(); err != nil {
		return nil, err
	} else if committed {
		return &RegisterOutput{
			Success: true,
			UserId:  userId,
		}, nil
	} else {
		return &RegisterOutput{
			Success: false,
			Message: "Failed to commit transaction due to conflicts",
		}, nil
	}
}

func registerMySQL(ctx context.Context, input *RegisterInput) (*RegisterOutput, error) {
	sess, err := client.StartSession(options.Session())
	if err != nil {
		return nil, err
	}
	defer sess.EndSession(ctx)

	db, err := sql.Open("mysql", "username:password@tcp(127.0.0.1:8081)/retwis")
	if err != nil {
		panic(err.Error())
	}
	//sess.WithTransaction calls MongoFetchAddCounter in utils/mongo.go
	//In MongoFetchAddCounter there is a bson call which I didnt understand
	//Couldn't find a mapping between it and sql
		db := client.Database("retwis")
		userIdValue, err := utils.MongoFetchAddCounter(sessCtx, db, "next_user_id", 1)
		if err != nil {
			return nil, err
		}
		//Also the insertion being done here is a bit confusing as in create_users.go
		// only username, password were inserted.
		query := "INSERT INTO users RetwisRegister(userId,username,password,auth,followers, followees,posts) VALUES (?, ?, ?, ?, ?, ?, ?)"
		ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelfunc()
		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			log.Printf("Error %s when preparing SQL statement", err)
			return nil, err
		}
		defer stmt.Close()
		res, err := stmt.ExecContext(ctx, userId, input.UserName, input.Password, fmt.Sprintf("%016x", rand.Uint64()), //bson.D{}, bson.D{}, bson.A{})
		if err != nil {
			log.Printf("Error %s when inserting row into users table", err)
			return nil, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			log.Printf("Error %s when finding rows affected", err)
			return nil, err
		}
		log.Printf("%d followers created ", rows)
		return userId, nil

	if err != nil {
		return &RegisterOutput{
			Success: false,
			Message: fmt.Sprintf("Mongo failed: %v", err),
		}, nil
	}

	return &RegisterOutput{
		Success: true,
		UserId:  userId.(string),
	}, nil
}

func (h *registerHandler) onRequest(ctx context.Context, input *RegisterInput) (*RegisterOutput, error) {
	switch h.kind {
	case "slib":
		return registerSlib(ctx, h.env, input)
	case "mongo":
		return registerMongo(ctx, h.client, input)
	default:
		panic(fmt.Sprintf("Unknown kind: %s", h.kind))
	}
}

func (h *registerHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &RegisterInput{}
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
