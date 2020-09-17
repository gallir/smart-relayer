package ifaceAthena

import (
	"encoding/json"
	"log"
	"testing"
	"time"
)

func TestAthena1(t *testing.T) {

	srv, err := New(Config{
		Profile:        "yourprofilename",
		Region:         "eu-west-1",
		OutputLocation: "s3://aws-athena-query-results-XXXXXX-eu-west-1",
	})
	if err != nil {
		t.Error(err)
		return
	}

	queryExecutionID, err := srv.Query("testexample", `
	SELECT * FROM "main"
	WHERE year = 2018
	AND month = 10
	AND day = 4 LIMIT 20`)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("ID: %s", queryExecutionID)

	time.Sleep(2 * time.Second)

	var rows []map[string]string
	nextToken := ""

	for {

		rows, nextToken, err = srv.Read(queryExecutionID, nextToken, 100)
		if err != nil {
			t.Error(err)
			return
		}

		for _, r := range rows {
			bolB, _ := json.Marshal(r)
			log.Println(string(bolB))
		}

		log.Printf("nextToken:  %s", nextToken)

		if nextToken == "" {
			return
		}

		time.Sleep(500 * time.Millisecond)

	}

}
