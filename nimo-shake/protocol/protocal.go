package protocal

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"nimo-shake/common"
)

// convert DynamoDB attributeValue to bson
type Converter interface {
	// run
	Run(input map[string]*dynamodb.AttributeValue) (interface{}, error)
}

func NewConverter(tp string) Converter {
	switch tp {
	case utils.ConvertTypeRaw:
		return new(RawConverter)
	case utils.ConvertTypeChange:
		return new(TypeConverter)
	case utils.ConvertMTypeChange:
		return new(MTypeConverter)
	case utils.ConvertTypeSame:
		return new(SameConverter)
	default:
		return nil
	}
}

type RawData struct {
	Size int         // fake size, only calculate real data
	Data interface{} // real data
}
