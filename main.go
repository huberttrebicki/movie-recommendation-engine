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
	router.GET("movies/:title/recommended", func(ctx *gin.Context) {
		title := ctx.Param("title")
		res, err := getMovieRecommendationsBasedOnTitle(c, driver, dbName, title)
		if err != nil {
			log.Fatalf("Error when executing query: %v", err)
		}
		ctx.JSON(200, gin.H{
			"basedOn": title,
			"movies":  res,
		})
	})
	router.Run()
}

func getMovieRecommendationsBasedOnTitle(ctx context.Context, driver neo4j.Driver, dbName, title string) ([]map[string]any, error) {
	params := map[string]any{
		"title": title,
	}

	// NOTE: query doesn't work the best with franchises, though dataset didn't have any connections
	cipher := `
	MATCH (target:Movie {title: $title})
	WITH target,
      [g IN apoc.convert.fromJsonList(target.genres) | g.name] AS targetGenres,
      [k IN apoc.convert.fromJsonList(target.keywords) | k.name] AS targetKeywords
    MATCH (rec:Movie)
    WHERE rec.id <> target.id AND rec.vote_count > 100
    WITH target, targetGenres, targetKeywords, rec,
      [g IN apoc.convert.fromJsonList(rec.genres) | g.name] AS recGenres,
      [k IN apoc.convert.fromJsonList(rec.keywords) | k.name] AS recKeywords
    WITH target, rec, recGenres, recKeywords,
      size([g IN recGenres WHERE g IN targetGenres]) AS sharedGenres,
      size([k IN recKeywords WHERE k IN targetKeywords]) AS sharedKeywords
    OPTIONAL MATCH (target)<-[:ACTED_IN|DIRECTED]-(p:Person)-[:ACTED_IN|DIRECTED]->(rec)
    WITH rec, recGenres, recKeywords, sharedGenres, sharedKeywords, count(p) AS sharedPeople
    WITH rec, recGenres, recKeywords, (sharedPeople * 5) + (sharedGenres * 2) + (sharedKeywords * 1) AS TotalScore
    WHERE TotalScore > 0
    ORDER BY TotalScore DESC, rec.vote_average DESC
    LIMIT 15
    RETURN
      rec.title AS Title,
      TotalScore,
      rec.vote_average AS Rating,
      recGenres AS Genres,
      rec.overview AS Overview,
      rec.runtime AS Runtime,
      recKeywords AS Keywords,
      rec.release_date AS ReleaseDate
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
