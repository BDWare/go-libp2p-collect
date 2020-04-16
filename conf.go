package collect

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/protocol"
)

type conf struct {
	requestProtocol  protocol.ID
	responseProtocol protocol.ID
	requestCacheSize int
}

func checkOptConfAndGetInnerConf(optConf *Conf) (inner *conf, err error) {
	if optConf.ProtocolPrefix == "" {
		err = fmt.Errorf("unexpected nil Prefix")
	}
	if optConf.RequestCacheSize < 0 {
		err = fmt.Errorf("unexpected negetive RequestCacheSize")
	}
	if err == nil {
		inner = &conf{
			requestProtocol:  protocol.ID(optConf.ProtocolPrefix + "/request"),
			responseProtocol: protocol.ID(optConf.ProtocolPrefix + "/response"),
			requestCacheSize: optConf.RequestCacheSize,
		}
	}
	return
}

// Conf is the static configuration of BasicPubSubCollector
type Conf struct {
	// ProtocolPrefix is the protocol name prefix
	ProtocolPrefix string `json:"protocol"`
	// RequestCacheSize .
	// RequestCache is used to store the request control message,
	// which is for response routing.
	RequestCacheSize int `json:"request_cache_size"`
	// ResponseCacheSize .
	// ResponseCache is used to deduplicate the response.
	ResponseCacheSize int `json:"response_cache_size"`
}

// MakeDefaultConf returns a default Conf instance
func MakeDefaultConf() Conf {
	return Conf{
		ProtocolPrefix:    "/basicpsc",
		RequestCacheSize:  512,
		ResponseCacheSize: 1024,
	}
}
