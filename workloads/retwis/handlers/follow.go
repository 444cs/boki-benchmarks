package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
	"strconv"

	"cs.utexas.edu/zjia/faas-retwis/utils"
	_ "github.com/go-sql-driver/mysql"

	"cs.utexas.edu/zjia/faas/slib/statestore"
	"cs.utexas.edu/zjia/faas/types"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type FollowInput struct {
	UserId     string `json:"userId"`
	FolloweeId string `json:"followeeId"`
	Unfollow   bool   `json:"unfollow,omitempty"`
}

type FollowOutput struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type followHandler struct {
	kind   string
	env    types.Environment
	client *mongo.Client
}

func NewSlibFollowHandler(env types.Environment) types.FuncHandler {
	return &followHandler{
		kind: "slib",
		env:  env,
	}
}

func NewMongoFollowHandler(env types.Environment) types.FuncHandler {
	return &followHandler{
		kind:   "mongo",
		env:    env,
		client: utils.CreateMongoClientOrDie(context.TODO()),
	}
}

func followSlib(ctx context.Context, env types.Environment, input *FollowInput) (*FollowOutput, error) {
	txn, err := statestore.CreateTxnEnv(ctx, env)
	if err != nil {
		return nil, err
	}

	userObj1 := txn.Object(fmt.Sprintf("userid:%s", input.UserId))
	if value, _ := userObj1.Get("username"); value.IsNull() {
		txn.TxnAbort()
		return &FollowOutput{
			Success: false,
			Message: fmt.Sprintf("Cannot find user with ID %s", input.UserId),
		}, nil
	}

	userObj2 := txn.Object(fmt.Sprintf("userid:%s", input.FolloweeId))
	if value, _ := userObj2.Get("username"); value.IsNull() {
		txn.TxnAbort()
		return &FollowOutput{
			Success: false,
			Message: fmt.Sprintf("Cannot find user with ID %s", input.FolloweeId),
		}, nil
	}

	if input.Unfollow {
		userObj1.Delete(fmt.Sprintf("followees.%s", input.FolloweeId))
		userObj2.Delete(fmt.Sprintf("followers.%s", input.UserId))
	} else {
		userObj1.SetBoolean(fmt.Sprintf("followees.%s", input.FolloweeId), true)
		userObj2.SetBoolean(fmt.Sprintf("followers.%s", input.UserId), true)
	}

	if committed, err := txn.TxnCommit(); err != nil {
		return nil, err
	} else if committed {
		return &FollowOutput{
			Success: true,
		}, nil
	} else {
		return &FollowOutput{
			Success: false,
			Message: "Failed to commit transaction due to conflicts",
		}, nil
	}
}

func followMongo(ctx context.Context, input *FollowInput) (*FollowOutput, error) {
	ctx, _ = context.WithTimeout(context.Background(), 300*time.Second)  
	db, err := sql.Open("mysql", "boki:retwisboki@tcp(boki.chou4ursccnw.us-east-2.rds.amazonaws.com:3306)/retwis")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if err != nil {
		return &FollowOutput{
			Success: false,
			Message: fmt.Sprintf("SQL failed: %v", err),
		}, nil
	}
	_, user_id := strconv.Atoi(input.UserId)
	_, followee_id := strconv.Atoi(input.FolloweeId)
	if input.Unfollow {
		_, err := db.ExecContext(ctx, "DELETE FROM follow WHERE user_id = ? AND followee_id = ?", user_id, followee_id)
		if err != nil {
			return nil, err
		}
		_, err = db.ExecContext(ctx, "UPDATE users SET followers = followers - 1 WHERE user_id = ?", user_id)
	if err != nil {
		panic(err)
	}
	_, err = db.ExecContext(ctx, "UPDATE users SET followees = followees - 1 WHERE user_id = ?", followee_id)
	if err != nil {
		panic(err)
	}
	
	fmt.Println("unfollowed")
	} else {
		_, err := db.ExecContext(ctx, "INSERT INTO follow (user_id, followee_id) VALUES(?, ?)", user_id, followee_id)
		if err != nil {
			return nil, err
		}
		_, err = db.ExecContext(ctx, "UPDATE users SET followers = followers + 1 WHERE user_id = ?", user_id)
	if err != nil {
		return nil, err
	}
	_, err = db.ExecContext(ctx, "UPDATE users SET followees = followees + 1 WHERE user_id = ?", followee_id)
	if err != nil {
		return nil, err
	}
	fmt.Println("followed")
	}
	if err != nil {
		return &FollowOutput{
			Success: false,
			Message: fmt.Sprintf("SQL failed: %v", err),
		}, nil
	}
	return &FollowOutput{
		Success: true,
	}, nil
}

func followMongo_bkp(ctx context.Context, client *mongo.Client, input *FollowInput) (*FollowOutput, error) {
	sess, err := client.StartSession(options.Session())
	if err != nil {
		return nil, err
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		coll := client.Database("retwis").Collection("users")
		user1Filter := bson.D{{"userId", input.UserId}}
		user2Filter := bson.D{{"userId", input.FolloweeId}}
		var user1Update bson.D
		var user2Update bson.D
		if input.Unfollow {
			user1Update = bson.D{{"$unset", bson.D{{fmt.Sprintf("followees.%s", input.FolloweeId), ""}}}}
			user2Update = bson.D{{"$unset", bson.D{{fmt.Sprintf("followers.%s", input.UserId), ""}}}}
		} else {
			user1Update = bson.D{{"$set", bson.D{{fmt.Sprintf("followees.%s", input.FolloweeId), true}}}}
			user2Update = bson.D{{"$set", bson.D{{fmt.Sprintf("followers.%s", input.UserId), true}}}}
		}
		if _, err := coll.UpdateOne(sessCtx, user1Filter, user1Update); err != nil {
			return nil, err
		}
		if _, err := coll.UpdateOne(sessCtx, user2Filter, user2Update); err != nil {
			return nil, err
		}
		return nil, nil
	}, utils.MongoTxnOptions())

	if err != nil {
		return &FollowOutput{
			Success: false,
			Message: fmt.Sprintf("Mongo failed: %v", err),
		}, nil
	}
	return &FollowOutput{
		Success: true,
	}, nil
}

func (h *followHandler) onRequest(ctx context.Context, input *FollowInput) (*FollowOutput, error) {
	if input.UserId == input.FolloweeId {
		return &FollowOutput{
			Success: false,
			Message: "userId and followeeId cannot be same",
		}, nil
	}

	switch h.kind {
	case "slib":
		return followSlib(ctx, h.env, input)
	case "mongo":
		//return followSQL(ctx, input)
		return followMongo(ctx, input)
	default:
		panic(fmt.Sprintf("Unknown kind: %s", h.kind))
	}
}

func (h *followHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &FollowInput{}
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
