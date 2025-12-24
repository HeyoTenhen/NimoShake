package utils

var (
	Version = "$"
)

// Check modes for data verification
// TODO: Add CheckModeBoth for bidirectional verification (check both directions in one run),
//       and CheckModeCount for count-only verification (based on bidirectional mode, since DynamoDB lacks precise count)
const (
	CheckModeMongoDB  = "mongodb"  // MongoDB as baseline: iterate MongoDB, verify against DynamoDB
	CheckModeDynamoDB = "dynamodb" // DynamoDB as baseline: iterate DynamoDB, verify against MongoDB (default mode)
)
