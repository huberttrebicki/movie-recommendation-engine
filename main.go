package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

func establishDbConnection(c context.Context) (neo4j.Driver, error) {
	dbUri := os.Getenv("DB_URI")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	if len(dbUri) == 0 || len(dbUser) == 0 || len(dbPassword) == 0 {
		return nil, fmt.Errorf("env variables are not configured correctly")
	}

	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth(dbUser, dbPassword, ""))
	if err != nil {
		return nil, err
	}

	err = driver.VerifyConnectivity(c)
	if err != nil {
		return nil, err
	}
	log.Print("Connection with db established successfully")

	return driver, nil
}

func main() {
	c := context.Background()
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error while loading env variables: %v", err)
	}
	driver, err := establishDbConnection(c)
	if err != nil {
		log.Fatalf("Error while establishing a connection: %v", err)
	}
	defer driver.Close(c)
	router := gin.Default()
	router.GET("/ping", func(ctx *gin.Context) {
		ctx.JSON(200, gin.H{
			"message": "pong",
		})
	})
	router.Run()
}

func getMovieRecommendationsBasedOnTitle() {

}
