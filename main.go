package main

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/urfave/cli/v2"
)

// 引数
var host string
var port string

func deleteDuplicate(ss ...[]string) []string {
	m := map[string]int{}
	for _, s := range ss {
		for _, v := range s {
			m[v]++
		}
	}
	res := []string{}
	for k, v := range m {
		if v == 1 {
			res = append(res, k)
		}
	}
	return res
}

func getIndices(es *elasticsearch.Client) []string {
	req := esapi.CatIndicesRequest{
		H: []string{"index"},
	}

	res, _ := req.Do(context.Background(), es)
	defer res.Body.Close()
	return toStringSlice(res.Body)
}

func getAliases(es *elasticsearch.Client) []string {
	req := esapi.CatAliasesRequest{
		H: []string{"index"},
	}

	res, _ := req.Do(context.Background(), es)
	defer res.Body.Close()
	return toStringSlice(res.Body)
}

func deleteIndex(index string, es *elasticsearch.Client) {
	req := esapi.IndicesDeleteRequest{
		Index: []string{index},
	}

	res, _ := req.Do(context.Background(), es)
	defer res.Body.Close()
}

func toStringSlice(body io.ReadCloser) []string {
	var (
		b1 = bytes.NewBuffer([]byte{})
		b2 = bytes.NewBuffer([]byte{})
	)

	tr := io.TeeReader(body, b1)
	defer func() { body = ioutil.NopCloser(b1) }()
	io.Copy(b2, tr)
	return strings.Split(b2.String(), "\n")
}

func deleteIndicesUnsetAliases(c *cli.Context) error {
	address := "http://" + host + ":" + port
	log.Println("connect to " + address)

	cfg := elasticsearch.Config{
		Addresses: []string{address},
	}
	es, _ := elasticsearch.NewClient(cfg)

	indices := getIndices(es)
	aliases := getAliases(es)
	deletes := deleteDuplicate(indices, aliases)

	for _, d := range deletes {
		log.Println("delete index : " + d)
		deleteIndex(d, es)
	}

	return nil
}

func main() {
	app := &cli.App{
		Name: "elasticsearch-index-cleaner-cli",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "host",
				Value:       "localhost",
				Aliases:     []string{"H"},
				Usage:       "connect to elasticsearch using `HOST`",
				Destination: &host,
			},
			&cli.StringFlag{
				Name:        "port",
				Value:       "9200",
				Aliases:     []string{"p"},
				Usage:       "connect to elasticsearch using `PORT`",
				Destination: &port,
			},
		},
		Action: deleteIndicesUnsetAliases,
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
