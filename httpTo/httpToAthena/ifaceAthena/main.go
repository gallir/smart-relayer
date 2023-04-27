package ifaceAthena

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/cespare/xxhash"
)

var (
	ErrInvalidResults = errors.New("Something goes really wrong, the results are not valid")
	ErrPending        = errors.New("The query is pending")
)

type Config struct {
	Region         string
	Profile        string
	OutputLocation string
}

type Athena struct {
	awsSvc *athena.Athena
	config Config
}

func New(c Config) (*Athena, error) {
	a := &Athena{
		config: c,
	}

	var err error
	var sess *session.Session

	if a.config.Profile != "" {
		sess, err = session.NewSessionWithOptions(session.Options{Profile: a.config.Profile})
	} else {
		sess, err = session.NewSession()
	}

	a.awsSvc = athena.New(sess, &aws.Config{Region: aws.String(a.config.Region)})
	return a, err
}

func (a *Athena) Query(database, querySQL string) (string, error) {

	utc := time.Now().UTC()
	token := fmt.Sprintf("%s-%d%d%d-%d%d-%d", database, utc.Year(), utc.Month(), utc.Day(), utc.Hour(), utc.Minute(), xxhash.Sum64String(querySQL))

	queryOutput, err := a.awsSvc.StartQueryExecution(&athena.StartQueryExecutionInput{
		ClientRequestToken: aws.String(token),
		QueryString:        aws.String(querySQL),
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String(database),
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(a.config.OutputLocation),
		},
	})
	if err != nil {
		return "", err
	}

	return *queryOutput.QueryExecutionId, nil
}

func (a *Athena) Read(queryExecutionId string, nextToken string, maxResults int64) ([]map[string]string, string, error) {

	queryExec, err := a.awsSvc.GetQueryExecution(&athena.GetQueryExecutionInput{
		QueryExecutionId: aws.String(queryExecutionId),
	})
	if err != nil {
		return nil, "", err
	}

	switch *queryExec.QueryExecution.Status.State {
	case "QUEUED", "RUNNING":
		return nil, "", ErrPending
	case "SUCCEEDED":
		// Just continue, reading the results of the query
		break
	default:
		// unknow status
		return nil, "", fmt.Errorf("Invalid query: %s", *queryExec.QueryExecution.Status.State)
	}

	getInput := &athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(queryExecutionId),
		MaxResults:       aws.Int64(maxResults),
		NextToken:        nil,
	}

	if nextToken != "" {
		getInput.NextToken = aws.String(nextToken)
	}

	results, err := a.awsSvc.GetQueryResults(getInput)
	if err != nil {
		return nil, "", err
	}

	if results == nil || results.ResultSet == nil || results.ResultSet.Rows == nil {
		return nil, "", ErrInvalidResults
	}

	colLen := len(results.ResultSet.ResultSetMetadata.ColumnInfo)

	newLines := make([]map[string]string, 0, len(results.ResultSet.Rows))
	if len(results.ResultSet.Rows) == 0 {
		return newLines, "", nil
	}

	// only skip the header if this is not a query for the next page
	startFrom := 1
	if nextToken != "" {
		startFrom = 0
	}

	for _, r := range results.ResultSet.Rows[startFrom:] {
		line := make(map[string]string, colLen)
		for i, d := range r.Data {
			if results.ResultSet.ResultSetMetadata.ColumnInfo[i].Name == nil {
				continue
			}
			keyName := *results.ResultSet.ResultSetMetadata.ColumnInfo[i].Name
			if d.VarCharValue != nil {
				line[keyName] = *d.VarCharValue
			} else {
				line[keyName] = ""
			}
		}
		newLines = append(newLines, line)
	}

	if results.NextToken == nil {
		return newLines, "", nil
	}
	return newLines, *results.NextToken, nil
}
