package pb

import "encoding/hex"

// RequestID .
type RequestID string

func (r RequestID) String() string {
	return hex.EncodeToString([]byte(r))
}
