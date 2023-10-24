package openSearch

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/go-faker/faker/v4"
	"github.com/go-faker/faker/v4/pkg/options"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"net/http"
	"os"
	"strings"
)

type Document struct {
	Email       string  `faker:"email"`
	Password    string  `faker:"password"`
	Name        string  `faker:"name"`
	Age         int     `faker:"boundary_start=5, boundary_end=50"`
	Height      int     `faker:"boundary_start=140, boundary_end=180"`
	PhoneNumber string  `faker:"phone_number"`
	Latitude    float32 `faker:"lat"`
	Longitude   float32 `faker:"long"`

	Tags    []Tag
	Article []Article
}

type Tag struct {
	Name string `faker:"word"`
}

type Article struct {
	ID        string `faker:"uuid_hyphenated"`
	Title     string `faker:"sentence"`
	Body      string `faker:"paragraph"`
	CreatedAt string `faker:"timestamp"`

	Tags []Tag
}

type LoginHistory struct {
	LoggedAt string `faker:"timestamp"`
	IPV4     string `faker:"ipv4"`
}

func NewClient() (*opensearch.Client, error) {
	address := os.Getenv("OPEN_SEARCH_ENDPOINT")
	user := os.Getenv("OPEN_SEARCH_MASTER_USER_NAME")
	pw := os.Getenv("OPEN_SEARCH_MASTER_USER_PASSWORD")

	return opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: []string{address},
		Username:  user,
		Password:  pw,
	})
}

func InsertDocuments(ctx context.Context, client *opensearch.Client, indexName string, count int) error {
	fmt.Println("Inserting documents...")

	pageSize := 500
	remainder := count % pageSize
	for i := 0; i < count/pageSize; i++ {
		_, err := insertFakeDocuments(ctx, client, indexName, pageSize)
		if err != nil {
			return err
		}
		fmt.Printf("Inserted: %d/%d\n", (i+1)*pageSize, count)
	}

	if remainder > 0 {
		_, err := insertFakeDocuments(ctx, client, indexName, remainder)
		if err != nil {
			return err
		}
	}

	fmt.Println("Insert completed")

	return nil
}

func insertFakeDocuments(ctx context.Context, client *opensearch.Client, indexName string, count int) (string, error) {
	jsonLd, err := makeFakeDocumentsJsonLD(indexName, count)
	if err != nil {
		return "", err
	}

	req := opensearchapi.BulkRequest{
		Body: strings.NewReader(jsonLd),
	}
	res, err := req.Do(ctx, client)
	if err != nil {
		return "", err
	}

	resBody := res.String()

	if res.StatusCode != 200 {
		return "", fmt.Errorf("status_code: %d, body: %s", res.StatusCode, resBody)
	}

	return resBody, nil
}

func makeFakeDocumentsJsonLD(indexName string, count int) (string, error) {
	action := `{"create":{"_index":"` + indexName + `"}}`
	var lines []string
	for i := 0; i < count; i++ {
		var doc Document
		err := faker.FakeData(&doc, options.WithRandomMapAndSliceMaxSize(20))
		if err != nil {
			return "", err
		}

		docJson, err := json.Marshal(doc)
		if err != nil {
			return "", err
		}

		lines = append(lines, action)
		lines = append(lines, string(docJson))
	}

	// add empty line to last
	lines = append(lines, "")

	return strings.Join(lines, "\n"), nil
}

func ListIndexes(ctx context.Context, client *opensearch.Client) (string, error) {
	req := opensearchapi.CatIndicesRequest{
		Format: "json",
	}

	res, err := req.Do(ctx, client)
	if err != nil {
		return "", err
	}

	respBody := res.String()

	if res.StatusCode != 200 {
		return "", fmt.Errorf("status_code: %d, body: %s", res.StatusCode, respBody)
	}

	return respBody, nil
}

func DeleteIndex(ctx context.Context, client *opensearch.Client, indexName string) (string, error) {
	req := opensearchapi.IndicesDeleteRequest{
		Index: []string{indexName},
	}

	res, err := req.Do(ctx, client)
	if err != nil {
		return "", err
	}

	respBody := res.String()

	if res.StatusCode != 200 {
		return "", fmt.Errorf("status_code: %d, body: %s", res.StatusCode, respBody)
	}

	return respBody, nil
}

func CreateIndexDisabledRefresh(ctx context.Context, client *opensearch.Client, indexName string) (string, error) {
	req := opensearchapi.IndicesCreateRequest{
		Index: indexName,
		Body: strings.NewReader(`{
  "settings": {
    "index": {
      "refresh_interval": "-1",
      "number_of_shards": 1,
      "number_of_replicas": 0
    }
  }
}
`),
	}

	res, err := req.Do(ctx, client)
	if err != nil {
		return "", err
	}

	respBody := res.String()

	if res.StatusCode != 200 {
		return "", fmt.Errorf("status_code: %d, body: %s", res.StatusCode, respBody)
	}

	return respBody, nil
}

func EnableRefreshIndex(ctx context.Context, client *opensearch.Client, indexName string) (string, error) {
	req := opensearchapi.IndicesPutSettingsRequest{
		Index: []string{indexName},
		Body: strings.NewReader(`{
  "index": {
    "refresh_interval": "1s"
  }
}
`),
	}

	res, err := req.Do(ctx, client)
	if err != nil {
		return "", err
	}

	respBody := res.String()

	if res.StatusCode != 200 {
		return "", fmt.Errorf("status_code: %d, body: %s", res.StatusCode, respBody)
	}

	return respBody, nil
}

func DisableRefreshIndex(ctx context.Context, client *opensearch.Client, indexName string) (string, error) {
	req := opensearchapi.IndicesPutSettingsRequest{
		Index: []string{indexName},
		Body: strings.NewReader(`{
  "index": {
    "refresh_interval": "-1"
  }
}
`),
	}

	res, err := req.Do(ctx, client)
	if err != nil {
		return "", err
	}

	respBody := res.String()

	if res.StatusCode != 200 {
		return "", fmt.Errorf("status_code: %d, body: %s", res.StatusCode, respBody)
	}

	return respBody, nil
}

func Search(ctx context.Context, client *opensearch.Client, indexName string) (string, error) {
	req := opensearchapi.SearchRequest{
		Index: []string{indexName},
		// Filter syntax: https://lucene.apache.org/core/2_9_4/queryparsersyntax.html
		Query: "Age:[10 TO 20]",
		//Query: "Email: *.com",
		Sort:         []string{"Age:asc"},
		Size:         opensearchapi.IntPtr(3),
		RequestCache: opensearchapi.BoolPtr(false),
		Pretty:       true,
	}

	res, err := req.Do(ctx, client)
	if err != nil {
		return "", err
	}

	respBody := res.String()

	if res.StatusCode != 200 {
		return "", fmt.Errorf("status_code: %d, body: %s", res.StatusCode, respBody)
	}

	return respBody, nil
}
