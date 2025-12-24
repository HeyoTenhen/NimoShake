package checker

import (
	"context"
	"encoding/json"
	"fmt"
	checkUtils "nimo-full-check/common"
	conf "nimo-full-check/configure"
	"os"
	"reflect"

	shakeUtils "nimo-shake/common"
	"nimo-shake/protocol"
	shakeQps "nimo-shake/qps"

	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/google/go-cmp/cmp"
	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// sortSlicesInData recursively sorts all slices in the data
// Used to handle the unordered nature of DynamoDB Set types
func sortSlicesInData(data interface{}) interface{} {
	if data == nil {
		return nil
	}

	switch v := data.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for k, val := range v {
			result[k] = sortSlicesInData(val)
		}
		return result

	case []interface{}:
		// first recursively process each element
		result := make([]interface{}, len(v))
		for i, elem := range v {
			result[i] = sortSlicesInData(elem)
		}
		// sort the slice
		sort.Slice(result, func(i, j int) bool {
			return fmt.Sprintf("%v", result[i]) < fmt.Sprintf("%v", result[j])
		})
		return result

	case []byte:
		// []byte should not be sorted, keep as is
		return v

	default:
		return v
	}
}

func convertToMap(data interface{}) interface{} {
	if data == nil {
		return nil
	}

	switch v := data.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for k, val := range v {
			result[k] = convertToMap(val)
		}
		return result

	case primitive.M:
		result := make(map[string]interface{})
		for k, val := range v {
			result[k] = convertToMap(val)
		}
		return result

	case primitive.D:
		result := make(map[string]interface{})
		for _, elem := range v {
			result[elem.Key] = convertToMap(elem.Value)
		}
		return result

	case primitive.ObjectID:
		return v.Hex()

	case primitive.Binary:
		// return binary data directly, let dynamodbattribute.Marshal handle it correctly
		return v.Data

	case []interface{}:
		var newSlice []interface{}
		for _, item := range v {
			newSlice = append(newSlice, convertToMap(item))
		}
		return newSlice

	case primitive.A:
		var newSlice []interface{}
		for _, item := range v {
			newSlice = append(newSlice, convertToMap(item))
		}
		return newSlice

	default:
		// use reflection to handle other slice types (e.g., []string, []int, etc.)
		rv := reflect.ValueOf(data)
		if rv.Kind() == reflect.Slice {
			// keep []byte unchanged, do not convert to []interface{}
			if rv.Type().Elem().Kind() == reflect.Uint8 {
				return data
			}
			length := rv.Len()
			result := make([]interface{}, length)
			for i := 0; i < length; i++ {
				result[i] = convertToMap(rv.Index(i).Interface())
			}
			return result
		}
		return v
	}
}

// compareMapData is a generic map data comparison function
// Returns whether the two maps are equal
func compareMapData(dataA, dataB map[string]interface{}) bool {

	opts := cmp.Options{
		cmp.FilterPath(func(p cmp.Path) bool {
			if len(p) > 0 {
				switch p.Last().(type) {
				case cmp.MapIndex, cmp.SliceIndex:
					return true
				}
			}
			return false
		}, cmp.Transformer("NormalizeNumbers", func(in interface{}) interface{} {
			switch v := in.(type) {
			case int, int32, int64, float32, float64:
				return reflect.ValueOf(v).Convert(reflect.TypeOf(float64(0))).Float()
			default:
				return in
			}
		})),
	}

	// 1. first perform ordered comparison (strict comparison)
	if cmp.Equal(dataA, dataB, opts) {
		LOG.Info("compareMapData: cmp.Equal returned true (but reflect.DeepEqual may have found diff)")
		return true
	}

	// 2. if ordered comparison fails, try sorting slices before comparing
	//    this handles the unordered nature of DynamoDB Set types
	dataASorted := sortSlicesInData(dataA).(map[string]interface{})
	dataBSorted := sortSlicesInData(dataB).(map[string]interface{})

	if cmp.Equal(dataASorted, dataBSorted, opts) {
		// equal after sorting means only Set type order differs, not a real difference
		return true
	}

	// 3. real difference, output detailed info (use original data diff for clarity)
	diff := cmp.Diff(dataA, dataB, opts)
	LOG.Warn("compareMapData failed, diff:\n%s", diff)

	// output type info for both sides to help locate type mismatch issues
	for key := range dataA {
		typeA := fmt.Sprintf("%T", dataA[key])
		typeB := "N/A"
		if val, exists := dataB[key]; exists {
			typeB = fmt.Sprintf("%T", val)
		}
		if typeA != typeB {
			LOG.Warn("  key[%s] type mismatch: src=%s, dst=%s", key, typeA, typeB)
		}
	}

	return false
}

func interIsEqual(dynamoData, convertedMongo interface{}) bool {
	// remove MongoDB's _id field
	if m, ok := convertedMongo.(map[string]interface{}); ok {
		delete(m, "_id")
	} else {
		LOG.Warn("don't have _id in mongodb document")
		return false
	}

	dynamoMap, okA := dynamoData.(map[string]interface{})
	mongoMap, okB := convertedMongo.(map[string]interface{})
	if !okA || !okB {
		LOG.Warn("data is not map type: dynamoData=%T, mongoData=%T", dynamoData, convertedMongo)
		return false
	}

	return compareMapData(dynamoMap, mongoMap)
}

const (
	fetcherChanSize = 512
	parserChanSize  = 4096
)

type KeyUnion struct {
	name  string
	tp    string
	union string
}

type DocumentChecker struct {
	id                 int
	ns                 shakeUtils.NS
	sourceConn         *dynamodb.DynamoDB
	mongoClient        *shakeUtils.MongoCommunityConn
	fetcherChan        chan *dynamodb.ScanOutput   // chan between fetcher and parser (for mongodb mode)
	fetcherMongoChan   chan map[string]interface{} // chan between fetcher and parser (for dynamodb mode)
	parserChan         chan protocol.RawData       // chan between parser and writer
	converter          protocol.Converter          // converter
	sampler            *Sample                     // use to sample
	primaryKeyWithType KeyUnion
	sortKeyWithType    KeyUnion
}

func NewDocumentChecker(id int, table string, dynamoSession *dynamodb.DynamoDB, mongoClient *shakeUtils.MongoCommunityConn) *DocumentChecker {
	converter := protocol.NewConverter(conf.Opts.ConvertType)
	if converter == nil {
		LOG.Error("documentChecker[%v] with table[%v] create converter failed", id, table)
		return nil
	}

	return &DocumentChecker{
		id:          id,
		sourceConn:  dynamoSession,
		mongoClient: mongoClient,
		converter:   converter,
		ns: shakeUtils.NS{
			Collection: table,
			Database:   conf.Opts.Id,
		},
	}
}

func (dc *DocumentChecker) String() string {
	return fmt.Sprintf("documentChecker[%v] with table[%s]", dc.id, dc.ns)
}

func (dc *DocumentChecker) Run() {
	// check outline
	if err := dc.checkOutline(); err != nil {
		LOG.Crashf("%s check outline failed[%v]", dc.String(), err)
	}

	LOG.Info("%s check outline finish, starts checking details with mode[%v]", dc.String(), conf.Opts.CheckMode)

	if conf.Opts.CheckMode == checkUtils.CheckModeMongoDB {
		// MongoDB as baseline: iterate MongoDB, verify against DynamoDB
		dc.runDynamoDBMode()
	} else {
		// DynamoDB as baseline (default mode): iterate DynamoDB, verify against MongoDB
		dc.runMongoDBMode()
	}
}

// runMongoDBMode uses DynamoDB as baseline: iterate DynamoDB, verify against MongoDB
func (dc *DocumentChecker) runMongoDBMode() {
	dc.fetcherChan = make(chan *dynamodb.ScanOutput, fetcherChanSize)
	dc.parserChan = make(chan protocol.RawData, parserChanSize)

	// start fetcher to fetch all data from DynamoDB
	go dc.fetcherDynamoDB()

	// start parser to get data from fetcher and write into executor
	go dc.parserDynamoDB()

	// start executor to check against MongoDB
	dc.executorMongo()
}

// runDynamoDBMode uses MongoDB as baseline: iterate MongoDB, verify against DynamoDB
func (dc *DocumentChecker) runDynamoDBMode() {
	dc.fetcherMongoChan = make(chan map[string]interface{}, fetcherChanSize)
	dc.parserChan = make(chan protocol.RawData, parserChanSize)

	// start fetcher to fetch all data from MongoDB
	go dc.fetcherMongo()

	// start parser to get data from fetcher and write into executor
	go dc.parserMongo()

	// start executor to check against DynamoDB
	dc.executorDynamoDB()
}

func (dc *DocumentChecker) fetcherDynamoDB() {
	LOG.Info("%s start fetcherDynamoDB", dc.String())

	qos := shakeQps.StartQoS(int(conf.Opts.QpsFull))
	defer qos.Close()

	// init nil
	var previousKey map[string]*dynamodb.AttributeValue
	for {
		<-qos.Bucket

		out, err := dc.sourceConn.Scan(&dynamodb.ScanInput{
			TableName:         aws.String(dc.ns.Collection),
			ExclusiveStartKey: previousKey,
			Limit:             aws.Int64(conf.Opts.QpsFullBatchNum),
		})
		if err != nil {
			// TODO check network error and retry
			LOG.Crashf("%s fetcher scan failed[%v]", dc.String(), err)
		}

		// pass result to parser
		dc.fetcherChan <- out

		previousKey = out.LastEvaluatedKey
		if previousKey == nil {
			// complete
			break
		}
	}

	LOG.Info("%s close fetcherDynamoDB", dc.String())
	close(dc.fetcherChan)
}

func (dc *DocumentChecker) parserDynamoDB() {
	LOG.Info("%s start parserDynamoDB", dc.String())

	for {
		data, ok := <-dc.fetcherChan
		if !ok {
			break
		}

		LOG.Debug("%s reads data[%v]", dc.String(), data)

		list := data.Items
		for _, ele := range list {
			out, err := dc.converter.Run(ele)
			if err != nil {
				LOG.Crashf("%s parses ele[%v] failed[%v]", dc.String(), ele, err)
			}

			// sample
			if dc.sampler.Hit() == false {
				continue
			}

			dc.parserChan <- out.(protocol.RawData)
		}
	}

	LOG.Info("%s close parserDynamoDB", dc.String())
	close(dc.parserChan)
}

func (dc *DocumentChecker) executorMongo() {
	LOG.Info("%s start executorMongo", dc.String())

	diffFile := fmt.Sprintf("%s/%s", conf.Opts.DiffOutputFile, dc.ns.Collection)
	f, err := os.Create(diffFile)
	if err != nil {
		LOG.Crashf("%s create diff output file[%v] failed", dc.String(), diffFile)
		return
	}

	for {
		data, ok := <-dc.parserChan
		if !ok {
			break
		}

		//var query map[string]interface{}
		query := make(map[string]interface{})
		if dc.primaryKeyWithType.name != "" {
			// find by union key
			if conf.Opts.ConvertType == shakeUtils.ConvertMTypeChange {
				query[dc.primaryKeyWithType.name] = data.Data.(map[string]interface{})[dc.primaryKeyWithType.name]
			} else {
				LOG.Crashf("unknown convert type[%v]", conf.Opts.ConvertType)
			}
		}
		if dc.sortKeyWithType.name != "" {
			if conf.Opts.ConvertType == shakeUtils.ConvertMTypeChange {
				query[dc.sortKeyWithType.name] = data.Data.(map[string]interface{})[dc.sortKeyWithType.name]
			} else {
				LOG.Crashf("unknown convert type[%v]", conf.Opts.ConvertType)
			}
		}

		LOG.Info("query: %v", query)

		// query
		var output, outputMap interface{}
		isSame := true
		err := dc.mongoClient.Client.Database(dc.ns.Database).Collection(dc.ns.Collection).
			FindOne(context.TODO(), query).Decode(&output)
		if err != nil {
			err = fmt.Errorf("target query failed[%v][%v][%v]", err, output, query)
			LOG.Error("%s %v", dc.String(), err)
		} else {
			outputMap = convertToMap(output)
			// also convert DynamoDB data types to ensure type consistency on both sides
			dynamoDataConverted := convertToMap(data.Data)
			isSame = interIsEqual(dynamoDataConverted, outputMap)
		}

		inputJson, _ := json.Marshal(data.Data)
		outputJson, _ := json.Marshal(outputMap)
		if err != nil {
			f.WriteString(fmt.Sprintf("compare src[%s] to dst[%s] failed: %v\n", inputJson, outputJson, err))
		} else if isSame == false {
			LOG.Warn("compare src[%s] and dst[%s] failed", inputJson, outputJson)
			f.WriteString(fmt.Sprintf("src[%s] != dst[%s]\n", inputJson, outputJson))
		}
	}

	LOG.Info("%s close executorMongo", dc.String())
	f.Close()

	// remove file if size == 0
	if fi, err := os.Stat(diffFile); err != nil {
		LOG.Warn("stat diffFile[%v] failed[%v]", diffFile, err)
		return
	} else if fi.Size() == 0 {
		if err := os.Remove(diffFile); err != nil {
			LOG.Warn("remove diffFile[%v] failed[%v]", diffFile, err)
		}
	}
}

func (dc *DocumentChecker) checkOutline() error {
	// describe dynamodb table
	out, err := dc.sourceConn.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(dc.ns.Collection),
	})
	if err != nil {
		return fmt.Errorf("describe table failed[%v]", err)
	}

	LOG.Info("describe table[%v] result: %v", dc.ns.Collection, out)

	// 1. check total number
	// dynamo count
	dynamoCount := out.Table.ItemCount

	// mongo count
	cnt, err := dc.mongoClient.Client.Database(dc.ns.Database).Collection(dc.ns.Collection).CountDocuments(context.Background(), bson.M{})
	if err != nil {
		return fmt.Errorf("get mongo count failed[%v]", err)
	}

	if *dynamoCount != cnt {
		// return fmt.Errorf("dynamo count[%v] != mongo count[%v]", *dynamoCount, cnt)
		LOG.Warn("dynamo count[%v] != mongo count[%v]", *dynamoCount, cnt)
	}

	// set sampler
	dc.sampler = NewSample(conf.Opts.Sample, cnt)

	// 2. check index
	// TODO

	// parse index
	// parse primary key with sort key
	allIndexes := out.Table.AttributeDefinitions
	primaryIndexes := out.Table.KeySchema

	// parse index type
	parseMap := shakeUtils.ParseIndexType(allIndexes)
	primaryKey, sortKey, err := shakeUtils.ParsePrimaryAndSortKey(primaryIndexes, parseMap)
	if err != nil {
		return fmt.Errorf("parse primary and sort key failed[%v]", err)
	}
	dc.primaryKeyWithType = KeyUnion{
		name:  primaryKey,
		tp:    parseMap[primaryKey],
		union: fmt.Sprintf("%s.%s", primaryKey, parseMap[primaryKey]),
	}
	dc.sortKeyWithType = KeyUnion{
		name:  sortKey,
		tp:    parseMap[sortKey],
		union: fmt.Sprintf("%s.%s", sortKey, parseMap[sortKey]),
	}

	return nil
}

// ==================== Methods for DynamoDB baseline mode ====================

// fetcherMongo fetches data by iterating through MongoDB
func (dc *DocumentChecker) fetcherMongo() {
	LOG.Info("%s start fetcherMongo", dc.String())

	ctx := context.Background()
	collection := dc.mongoClient.Client.Database(dc.ns.Database).Collection(dc.ns.Collection)

	// use cursor to iterate through MongoDB
	findOptions := options.Find()
	findOptions.SetBatchSize(int32(conf.Opts.QpsFullBatchNum))

	cursor, err := collection.Find(ctx, bson.M{}, findOptions)
	if err != nil {
		LOG.Crashf("%s fetcherMongo find failed[%v]", dc.String(), err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			LOG.Crashf("%s fetcherMongo decode failed[%v]", dc.String(), err)
		}

		// convert to map[string]interface{}
		result := make(map[string]interface{})
		for k, v := range doc {
			result[k] = convertToMap(v)
		}

		dc.fetcherMongoChan <- result
	}

	if err := cursor.Err(); err != nil {
		LOG.Crashf("%s fetcherMongo cursor error[%v]", dc.String(), err)
	}

	LOG.Info("%s close fetcherMongo", dc.String())
	close(dc.fetcherMongoChan)
}

// parserMongo parses MongoDB data
func (dc *DocumentChecker) parserMongo() {
	LOG.Info("%s start parserMongo", dc.String())

	for {
		data, ok := <-dc.fetcherMongoChan
		if !ok {
			break
		}

		LOG.Debug("%s parserMongo reads data[%v]", dc.String(), data)

		// sample
		if dc.sampler.Hit() == false {
			continue
		}

		// calculate data size
		dataJson, _ := json.Marshal(data)
		size := len(dataJson)

		dc.parserChan <- protocol.RawData{
			Size: size,
			Data: data,
		}
	}

	LOG.Info("%s close parserMongo", dc.String())
	close(dc.parserChan)
}

// executorDynamoDB performs verification using DynamoDB as baseline
func (dc *DocumentChecker) executorDynamoDB() {
	LOG.Info("%s start executorDynamoDB", dc.String())

	diffFile := fmt.Sprintf("%s/%s", conf.Opts.DiffOutputFile, dc.ns.Collection)
	f, err := os.Create(diffFile)
	if err != nil {
		LOG.Crashf("%s create diff output file[%v] failed", dc.String(), diffFile)
		return
	}

	qos := shakeQps.StartQoS(int(conf.Opts.QpsFull))
	defer qos.Close()

	for {
		data, ok := <-dc.parserChan
		if !ok {
			break
		}

		mongoData := data.Data.(map[string]interface{})

		// build DynamoDB query Key
		key := make(map[string]*dynamodb.AttributeValue)

		// get primary key value
		if dc.primaryKeyWithType.name != "" {
			pkValue := mongoData[dc.primaryKeyWithType.name]
			av, err := dynamodbattribute.Marshal(pkValue)
			if err != nil {
				LOG.Error("%s marshal primary key failed[%v]", dc.String(), err)
				continue
			}
			key[dc.primaryKeyWithType.name] = av
		}

		// get sort key value
		if dc.sortKeyWithType.name != "" {
			skValue := mongoData[dc.sortKeyWithType.name]
			av, err := dynamodbattribute.Marshal(skValue)
			if err != nil {
				LOG.Error("%s marshal sort key failed[%v]", dc.String(), err)
				continue
			}
			key[dc.sortKeyWithType.name] = av
		}

		LOG.Info("query dynamodb with key: %v", key)

		// QPS rate limiting
		<-qos.Bucket

		// query DynamoDB
		result, err := dc.sourceConn.GetItem(&dynamodb.GetItemInput{
			TableName: aws.String(dc.ns.Collection),
			Key:       key,
		})

		var dynamoData map[string]interface{}
		isSame := true

		if err != nil {
			err = fmt.Errorf("dynamodb query failed[%v][key:%v]", err, key)
			LOG.Error("%s %v", dc.String(), err)
		} else if result.Item == nil {
			err = fmt.Errorf("dynamodb item not found[key:%v]", key)
			LOG.Error("%s %v", dc.String(), err)
		} else {
			// convert DynamoDB result to map
			if err := dynamodbattribute.UnmarshalMap(result.Item, &dynamoData); err != nil {
				LOG.Error("%s unmarshal dynamodb result failed[%v]", dc.String(), err)
				continue
			}

			// convert DynamoDB data types, handle special types like []string
			dynamoDataConverted := convertToMap(dynamoData).(map[string]interface{})

			// compare data (after removing MongoDB's _id field)
			mongoDataCopy := make(map[string]interface{})
			for k, v := range mongoData {
				if k != "_id" {
					mongoDataCopy[k] = v
				}
			}

			isSame = dc.compareData(dynamoDataConverted, mongoDataCopy)
		}

		mongoJson, _ := json.Marshal(mongoData)
		dynamoJson, _ := json.Marshal(dynamoData)

		if err != nil {
			f.WriteString(fmt.Sprintf("compare src[%s] to dst[%s] failed: %v\n", mongoJson, dynamoJson, err))
		} else if !isSame {
			LOG.Warn("compare src[%s] and dst[%s] failed", mongoJson, dynamoJson)
			f.WriteString(fmt.Sprintf("src[%s] != dst[%s]\n", mongoJson, dynamoJson))
		}
	}

	LOG.Info("%s close executorDynamoDB", dc.String())
	f.Close()

	// remove file if size == 0
	if fi, err := os.Stat(diffFile); err != nil {
		LOG.Warn("stat diffFile[%v] failed[%v]", diffFile, err)
		return
	} else if fi.Size() == 0 {
		if err := os.Remove(diffFile); err != nil {
			LOG.Warn("remove diffFile[%v] failed[%v]", diffFile, err)
		}
	}
}

// compareData compares whether two map data are equal
func (dc *DocumentChecker) compareData(dynamoData, mongoData map[string]interface{}) bool {
	return compareMapData(dynamoData, mongoData)
}
