package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func u68Hash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// TestU68BodiesGoldenBytes: [GUARD any byte change in a command body]. The Raft command
// bodies and the state encoding must stay byte-identical across the BSON library swap.
func TestU68BodiesGoldenBytes(t *testing.T) {
	var clusterID [16]byte
	for i := range clusterID {
		clusterID[i] = byte(0xA0 + i)
	}
	placing := make([]byte, 1024)
	for i := range placing {
		placing[i] = byte(i * 7)
	}
	self := Member{ID: "node-a", Incarnation: 42, DataAddr: "node-a:7000"}
	witness := self
	witness.Witness = true
	raw := func(name string, body []byte) [2]string { return [2]string{name, u68Hash(body)} }
	state, err := EncodeClusterState(stateFull())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range [][2]string{
		raw("bootstrap", BootstrapBody(clusterID, self, 1)),
		raw("bootstrap-witness", BootstrapBody(clusterID, witness, 1)),
		raw("register", RegisterMemberBody(witness, placing, StateRange{Min: 1, Max: 1, Writes: 1})),
		raw("voters-on", SetVotersBody("node-a", true)),
		raw("voters-off", SetVotersBody("node-a", false)),
		raw("state-full", state),
		raw("state-given-order", encodeClusterStateInGivenOrder(stateFull())),
	} {
		if want, ok := u68Golden[c[0]]; !ok || want != c[1] {
			t.Errorf("%s: sha256 %s, want %s", c[0], c[1], want)
		}
	}
}

var u68Golden = map[string]string{
	"bootstrap":         "83936f1b974a5ccfa16da15eaa67dc90366ace046b72ad2485538cbfd5455032",
	"bootstrap-witness": "b967857c75b2170c07edc1158c5fa3a894c5a52257a3226b93c81e8556186cba",
	"register":          "bc2a4eb41f9ef44be3eb4fce699d20cba1e28caa9333f7281a8ee7df09b23bce",
	"voters-on":         "e8394d0009b8dfeb7a4147be3ba81f666a7c5727dc34a426885e5ad9ada3c803",
	"voters-off":        "248c88b4964e7a2c3f02754109af0961362e71e5955db9889a1cf39220bcbc56",
	"state-full":        "e555e515fe4d2cc4c017707195e5be7d36bf1a0fc14c3a4a60ea95a8bf4ac45d",
	"state-given-order": "e555e515fe4d2cc4c017707195e5be7d36bf1a0fc14c3a4a60ea95a8bf4ac45d",
}
