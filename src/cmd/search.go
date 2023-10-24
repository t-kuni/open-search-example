package main

import (
	"context"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/t-kuni/aws-open-search-example/internal/openSearch"
	"os"
)

func main() {
	ctx := context.Background()

	godotenv.Load()

	client, err := openSearch.NewClient()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	indexName := "go-test-1"

	respBody, err := openSearch.Search(ctx, client, indexName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	println(respBody)
}
