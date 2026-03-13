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

	dbName := os.Getenv("DB_NAME")
	router := gin.Default()
	router.GET("/directors/:name/movies", func(ctx *gin.Context) {
		directorName := ctx.Param("name")
		res, err := getMoviesBasedOnDirector(c, driver, dbName, directorName)
		if err != nil {
			log.Fatalf("Error when executing query: %v", err)
		}
		ctx.JSON(200, gin.H{
			"director": directorName,
			"movies":   res,
		})
	})
	router.GET("/actors/:name/movies", func(ctx *gin.Context) {
		actorName := ctx.Param("name")
		res, err := getMoviesBasedOnActor(c, driver, dbName, actorName)
		if err != nil {
			log.Fatalf("Error when executing query: %v", err)
		}
		ctx.JSON(200, gin.H{
			"actor":  actorName,
			"movies": res,
		})
	})
	router.Run()
}

func getMovieRecommendationsBasedOnTitle(ctx context.Context, driver neo4j.Driver, title string) {
}

func getMoviesBasedOnActor(ctx context.Context, driver neo4j.Driver, dbName, a string) ([]map[string]any, error) {
	params := map[string]any{
		"actorName": a,
	}

	cipher := `
	MATCH (d:Person {name: $actorName})-[:ACTED_IN]->(m:Movie)
	WHERE m.vote_count > 100
	RETURN m.title AS Title,
           m.vote_average AS Rating,
           [genre IN apoc.convert.fromJsonList(m.genres) | genre.name] AS Genres,
           m.overview AS Overview,
           m.runtime AS Runtime,
           [keyword IN apoc.convert.fromJsonList(m.keywords) | keyword.name] AS Keywords,
           m.release_date AS ReleaseDate
	ORDER BY Rating DESC, m.popularity DESC
	LIMIT 20
	`

	res, err := neo4j.ExecuteQuery(ctx, driver, cipher, params, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(dbName))
	if err != nil {
		return nil, err
	}

	var movies []map[string]any
	for _, record := range res.Records {
		movies = append(movies, record.AsMap())
	}
	return movies, nil
}

func getMoviesBasedOnDirector(ctx context.Context, driver neo4j.Driver, dbName, d string) ([]map[string]any, error) {
	params := map[string]any{
		"directorName": d,
	}

	cipher := `
	MATCH (d:Person {name: $directorName})-[:DIRECTED]->(m:Movie)
	WHERE m.vote_count > 100
	RETURN m.title AS Title,
           m.vote_average AS Rating,
           [genre IN apoc.convert.fromJsonList(m.genres) | genre.name] AS Genres,
           m.overview AS Overview,
           m.runtime AS Runtime,
           [keyword IN apoc.convert.fromJsonList(m.keywords) | keyword.name] AS Keywords,
           m.release_date AS ReleaseDate
	ORDER BY Rating DESC, m.popularity DESC
	LIMIT 20
	`

	res, err := neo4j.ExecuteQuery(ctx, driver, cipher, params, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(dbName))
	if err != nil {
		return nil, err
	}

	var movies []map[string]any
	for _, record := range res.Records {
		movies = append(movies, record.AsMap())
	}
	return movies, nil
}
