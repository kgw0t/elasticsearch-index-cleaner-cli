package main

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

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

func isDeleteTargetIndexName(name string, now time.Time) bool {
	times := strings.Split(name, "-")
	data := strings.Split(times[len(times)-1], ".")
	if len(data) != 6 {
		return false
	}

	Y, erry := strconv.Atoi(data[0])
	M, errM := strconv.Atoi(data[1])
	d, errd := strconv.Atoi(data[2])
	h, errh := strconv.Atoi(data[3])
	m, errm := strconv.Atoi(data[4])
	s, errs := strconv.Atoi(data[5])
	if erry != nil || errM != nil || errd != nil || errh != nil || errm != nil || errs != nil {
		return false
	}

	t := time.Date(Y, time.Month(M), d, h, m, s, 0, time.Local)

	return !t.After(now)
}

func deleteIndicesUnsetAliases(c *cli.Context) error {
	address := "http://" + host + ":" + port
	log.Println("connect to", address)

	now := time.Now()
	log.Println("delete index created now :", now.String())

	cfg := elasticsearch.Config{
		Addresses: []string{address},
	}
	es, _ := elasticsearch.NewClient(cfg)

	indices := getIndices(es)
	aliases := getAliases(es)
	deletes := deleteDuplicate(indices, aliases)

	for _, d := range deletes {
		if isDeleteTargetIndexName(d, now) {
			log.Println("delete index :", d)
			deleteIndex(d, es)
		} else {
			log.Println("non delete target index :", d)
		}

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
