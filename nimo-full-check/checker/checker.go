package checker

import (
	"context"
	"fmt"
	"sync"

	checkUtils "nimo-full-check/common"
	conf "nimo-full-check/configure"
	shakeUtils "nimo-shake/common"
	shakeFilter "nimo-shake/filter"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
)

type Checker struct {
	dynamoSession *dynamodb.DynamoDB
	mongoClient   *shakeUtils.MongoCommunityConn
}

func NewChecker(dynamoSession *dynamodb.DynamoDB, mongoClient *shakeUtils.MongoCommunityConn) *Checker {
	return &Checker{
		dynamoSession: dynamoSession,
		mongoClient:   mongoClient,
	}
}

func (c *Checker) Run() error {
	LOG.Info("start checker with mode[%v]", conf.Opts.CheckMode)

	var tableList []string
	var err error

	if conf.Opts.CheckMode == checkUtils.CheckModeMongoDB {
		// MongoDB as baseline: fetch table list from MongoDB, verify against DynamoDB
		tableList, err = c.fetchMongoTableList()
		if err != nil {
			return fmt.Errorf("fetch mongodb table list failed[%v]", err)
		}

		// check if tables exist in DynamoDB
		if err := c.checkTableExistInDynamoDB(tableList); err != nil {
			return fmt.Errorf("check table exist in dynamodb failed[%v]", err)
		}
	} else {
		// DynamoDB as baseline: fetch table list from DynamoDB, verify against MongoDB
		rawTableList, err := shakeUtils.FetchTableList(c.dynamoSession)
		if err != nil {
			return fmt.Errorf("fetch dynamodb table list failed[%v]", err)
		}
		LOG.Info("finish fetching dynamodb table list: %v", rawTableList)

		tableList = shakeFilter.FilterList(rawTableList)

		// check if tables exist in MongoDB
		if err := c.checkTableExistInMongoDB(tableList); err != nil {
			return fmt.Errorf("check table exist in mongodb failed[%v]", err)
		}
	}

	LOG.Info("filter table list: %v", tableList)

	// reset parallel if needed
	parallel := conf.Opts.Parallel
	if parallel > len(tableList) {
		parallel = len(tableList)
	}

	execChan := make(chan string, len(tableList))
	for _, table := range tableList {
		execChan <- table
	}

	var wg sync.WaitGroup
	wg.Add(len(tableList))
	for i := 0; i < parallel; i++ {
		go func(id int) {
			for {
				table, ok := <-execChan
				if !ok {
					break
				}

				LOG.Info("documentChecker[%v] starts checking table[%v] with mode[%v]", id, table, conf.Opts.CheckMode)
				dc := NewDocumentChecker(id, table, c.dynamoSession, c.mongoClient)
				dc.Run()

				LOG.Info("documentChecker[%v] finishes checking table[%v]", id, table)
				wg.Done()
			}
		}(i)
	}
	wg.Wait()

	LOG.Info("all documentCheckers finish")
	return nil
}

// fetchMongoTableList fetches collection list from MongoDB
func (c *Checker) fetchMongoTableList() ([]string, error) {
	LOG.Info("start fetching mongodb collection list")
	collections, err := c.mongoClient.Client.Database(conf.Opts.Id).ListCollectionNames(context.TODO(), bson.M{})
	if err != nil {
		return nil, fmt.Errorf("get mongodb collection names error[%v]", err)
	}
	LOG.Info("finish fetching mongodb collection list: %v", collections)

	// apply filter rules
	tableList := shakeFilter.FilterList(collections)
	return tableList, nil
}

// checkTableExistInMongoDB checks if tables exist in MongoDB (original logic)
func (c *Checker) checkTableExistInMongoDB(tableList []string) error {
	collections, err := c.mongoClient.Client.Database(conf.Opts.Id).ListCollectionNames(context.TODO(), bson.M{})
	if err != nil {
		return fmt.Errorf("get mongodb collection names error[%v]", err)
	}

	LOG.Info("mongodb collections: %v", collections)

	collectionsMp := shakeUtils.StringListToMap(collections)
	notExist := make([]string, 0)
	for _, table := range tableList {
		if _, ok := collectionsMp[table]; !ok {
			notExist = append(notExist, table)
		}
	}

	if len(notExist) != 0 {
		return fmt.Errorf("table not exist in mongodb: %v", notExist)
	}
	return nil
}

// checkTableExistInDynamoDB checks if tables exist in DynamoDB
func (c *Checker) checkTableExistInDynamoDB(tableList []string) error {
	dynamoTables, err := shakeUtils.FetchTableList(c.dynamoSession)
	if err != nil {
		return fmt.Errorf("fetch dynamodb table list error[%v]", err)
	}

	LOG.Info("dynamodb tables: %v", dynamoTables)

	tablesMp := shakeUtils.StringListToMap(dynamoTables)
	notExist := make([]string, 0)
	for _, table := range tableList {
		if _, ok := tablesMp[table]; !ok {
			notExist = append(notExist, table)
		}
	}

	if len(notExist) != 0 {
		return fmt.Errorf("table not exist in dynamodb: %v", notExist)
	}

	// additional check for each table's details (ensure table is accessible)
	for _, table := range tableList {
		_, err := c.dynamoSession.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			return fmt.Errorf("describe dynamodb table[%v] failed[%v]", table, err)
		}
	}

	return nil
}
