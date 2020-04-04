package pb

//go:generate go get -u github.com/gogo/protobuf/protoc-gen-gogofaster
//go:generate protoc -I../../../common/proto -I. --gogofaster_out=. pubsubcollect.proto
