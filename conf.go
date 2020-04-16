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
