package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"
	_ "github.com/go-sql-driver/mysql"

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

func registerMongo(ctx context.Context, input *RegisterInput) (*RegisterOutput, error) {
	db, err := sql.Open("mysql", "boki:retwisboki@tcp(boki.chou4ursccnw.us-east-2.rds.amazonaws.com:3306)/retwis")
	ctx, _ = context.WithTimeout(context.Background(), 300*time.Second)  
	if err != nil {
		panic(err)
	} 
	res, err := db.ExecContext(ctx, "INSERT INTO users (username, password, auth, followers, followees, posts) VALUES (?, ?, ?, ?, ?, ?)", input.UserName, input.Password, fmt.Sprintf("%016x", rand.Uint64()), 0, 0, 0)
	if err != nil {
		return &RegisterOutput{
			Success: false,
			Message: fmt.Sprintf("SQL failed: %v", err),
		}, nil
	}
	
	fmt.Println("registered")
	userId, err := res.LastInsertId()
	if err != nil {
		return &RegisterOutput{
			Success: false,
			Message: fmt.Sprintf("SQL failed: %v", err),
		}, nil
	}
	str_user_id := strconv.FormatInt(userId, 10)
	defer db.Close()
	return &RegisterOutput{
		Success: true,
		UserId:  str_user_id,
	}, nil
}

func registerMongo_bkp(ctx context.Context, client *mongo.Client, input *RegisterInput) (*RegisterOutput, error) {
	sess, err := client.StartSession(options.Session())
	if err != nil {
		return nil, err
	}
	defer sess.EndSession(ctx)

	userId, err := sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		db := client.Database("retwis")
		userIdValue, err := utils.MongoFetchAddCounter(sessCtx, db, "next_user_id", 1)
		if err != nil {
			return nil, err
		}

		userId := fmt.Sprintf("%08x", userIdValue)
		userBson := bson.D{
			{"userId", userId},
			{"username", input.UserName},
			{"password", input.Password},
			{"auth", fmt.Sprintf("%016x", rand.Uint64())},
			{"followers", bson.D{}},
			{"followees", bson.D{}},
			{"posts", bson.A{}},
		}
		if _, err := db.Collection("users").InsertOne(sessCtx, userBson); err != nil {
			return nil, err
		}

		return userId, nil
	}, utils.MongoTxnOptions())

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
		//return registerSQL(ctx, input)
		return registerMongo(ctx, input)
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
