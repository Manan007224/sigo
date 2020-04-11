package store

import (
	"fmt"
	"time"
)

const (
	RFCTimeLayout = time.RFC3339
)

var (
	protoMarshalErr    = fmt.Errorf("error in marshaling job")
	protoUnMarshalErr  = fmt.Errorf("error in unmarshalling job")
	strToFloatParseErr = fmt.Errorf("error in converting timestamp to float")
)
