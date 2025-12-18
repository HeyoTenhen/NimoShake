package protocal

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	conf "nimo-shake/configure"
	LOG "nimo-shake/third_party/log4go"
)

type MTypeConverter struct {
}

func (tc *MTypeConverter) Run(input map[string]*dynamodb.AttributeValue) (interface{}, error) {
	funcStartT := time.Now()

	outLen := 0
	for key, value := range input {
		outLen = outLen + len(key) + len(value.String())
	}

	out := new(interface{})
	if err := dynamodbattribute.UnmarshalMap(input, out); err == nil {

		for key, value := range (*out).(map[string]interface{}) {
			if key == "_id" {
				delete((*out).(map[string]interface{}), key)
				((*out).(map[string]interface{}))[conf.ConvertIdFunc(key)] = value
			}
		}

		LOG.Debug("Run_func input[%v] out[%v] out_len[%v] duration[%v]",
			input, *out, outLen, time.Since(funcStartT))

		return RawData{outLen, *out}, nil
	} else {
		LOG.Debug("Run_func input[%v] out[%v] err[%v]", input, *out, err)
	}

	return RawData{}, fmt.Errorf("parse failed, return nil")
}
