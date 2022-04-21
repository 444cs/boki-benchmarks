package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	_ "github.com/go-sql-driver/mysql"

	"cs.utexas.edu/zjia/faas-retwis/utils"

	"cs.utexas.edu/zjia/faas/slib/statestore"
	"cs.utexas.edu/zjia/faas/types"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type ProfileInput struct {
	UserId string `json:"userId"`
}

type ProfileOutput struct {
	Success      bool   `json:"success"`
	Message      string `json:"message,omitempty"`
	UserName     string `json:"username,omitempty"`
	NumFollowers int    `json:"numFollowers"`
	NumFollowees int    `json:"numFollowees"`
	NumPosts     int    `json:"numPosts"`
}

type profileHandler struct {
	kind   string
	env    types.Environment
	client *mongo.Client
}

func NewSlibProfileHandler(env types.Environment) types.FuncHandler {
	return &profileHandler{
		kind: "slib",
		env:  env,
	}
}

func NewMongoProfileHandler(env types.Environment) types.FuncHandler {
	return &profileHandler{
		kind:   "mongo",
		env:    env,
		client: utils.CreateMongoClientOrDie(context.TODO()),
	}
}

func profileSlib(ctx context.Context, env types.Environment, input *ProfileInput) (*ProfileOutput, error) {
	output := &ProfileOutput{Success: true}

	store := statestore.CreateEnv(ctx, env)
	userObj := store.Object(fmt.Sprintf("userid:%s", input.UserId))
	if value, _ := userObj.Get("username"); !value.IsNull() {
		output.UserName = value.AsString()
	} else {
		return &ProfileOutput{
			Success: false,
			Message: fmt.Sprintf("Cannot find user with ID %s", input.UserId),
		}, nil
	}
	if value, _ := userObj.Get("followers"); !value.IsNull() {
		output.NumFollowers = value.Size()
	}
	if value, _ := userObj.Get("followees"); !value.IsNull() {
		output.NumFollowees = value.Size()
	}
	if value, _ := userObj.Get("posts"); !value.IsNull() {
		output.NumPosts = value.Size()
	}

	return output, nil
}

func profileMongo(ctx context.Context, input *ProfileInput) (*ProfileOutput, error) {
	db, err := sql.Open("mysql", "boki:retwisboki@tcp(boki.chou4ursccnw.us-east-2.rds.amazonaws.com:3306)/retwis")
	if err != nil {
		return &ProfileOutput{
			Success: false,
			Message: fmt.Sprintf("SQL failed: %v", err),
		}, nil
	} else if err = db.Ping(); err != nil {
		return &ProfileOutput{
			Success: false,
			Message: fmt.Sprintf("SQL failed: %v", err),
		}, nil
	}
	query := "SELECT username, followers, followees, posts FROM users WHERE user_id = ?"
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return &ProfileOutput{
			Success: false,
			Message: fmt.Sprintf("SQL failed: %v", err),
		}, nil
	}
	defer stmt.Close()
	var username string
	var followers int
	var followees int
	var posts int
	user_id, err := strconv.Atoi(input.UserId)
	row := stmt.QueryRowContext(ctx, user_id)
	if err := row.Scan(&username, &followers, &followees, &posts); err != nil {
		return &ProfileOutput{
			Success: false,
			Message: fmt.Sprintf("SQL failed: %v", err),
		}, nil
	}
	output := &ProfileOutput{Success: true}
	output.UserName = username
	output.NumFollowers = followers
	output.NumFollowees = followees
	output.NumPosts = posts
	fmt.Println("display stuff out")
	return output, nil
}

func profileMongo_bkp(ctx context.Context, client *mongo.Client, input *ProfileInput) (*ProfileOutput, error) {
	db := client.Database("retwis")

	var user bson.M
	if err := db.Collection("users").FindOne(ctx, bson.D{{"userId", input.UserId}}).Decode(&user); err != nil {
		return &ProfileOutput{
			Success: false,
			Message: fmt.Sprintf("Mongo failed: %v", err),
		}, nil
	}

	output := &ProfileOutput{Success: true}
	if value, ok := user["username"].(string); ok {
		output.UserName = value
	}
	if value, ok := user["followers"].(bson.M); ok {
		output.NumFollowers = len(value)
	}
	if value, ok := user["followees"].(bson.M); ok {
		output.NumFollowees = len(value)
	}
	if value, ok := user["posts"].(bson.A); ok {
		output.NumPosts = len(value)
	}

	return output, nil
}

func (h *profileHandler) onRequest(ctx context.Context, input *ProfileInput) (*ProfileOutput, error) {
	switch h.kind {
	case "slib":
		return profileSlib(ctx, h.env, input)
	case "mongo":
		//return profileSQL(ctx, input)
		return profileMongo(ctx, input)
	default:
		panic(fmt.Sprintf("Unknown kind: %s", h.kind))
	}
}

func (h *profileHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &ProfileInput{}
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
