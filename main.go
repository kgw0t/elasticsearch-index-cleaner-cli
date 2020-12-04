package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func unique(ss ...[]string) []string {
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

func main() {
	// elasticsearch client 接続
	es, _ := elasticsearch.NewDefaultClient()

	indices := getIndices(es)
	// fmt.Println(indices)

	aliases := getAliases(es)
	// fmt.Println(aliases)

	deletes := unique(indices, aliases)
	// fmt.Println(deletes)

	for _, d := range deletes {
		fmt.Println("delete : " + d)
		deleteIndex(d, es)
	}
}
