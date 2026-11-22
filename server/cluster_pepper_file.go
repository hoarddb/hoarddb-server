package server

import (
	"fmt"

	"github.com/hoarddb/hoarddb-server/clusterpepper"
)

// clusterPepperFile is auth.cluster.pepper in a node's data directory, read and written through
// the credential-file table (ring user store spec 5.4): sealed under the node master when the node
// seals its credentials, else the 0600 plaintext form. The ring node holds it through
// wire.PepperFile; it is one 32-byte file per node, nothing per bucket.
type clusterPepperFile struct {
	keys    CredentialKeys
	dataDir string
}

func newClusterPepperFile(keys CredentialKeys, dataDir string) clusterPepperFile {
	return clusterPepperFile{keys: keys, dataDir: dataDir}
}

// Load reads the pepper. A missing file is an error that matches fs.ErrNotExist; a sealed file
// this start cannot open is the credential refusal, with its restore text.
func (f clusterPepperFile) Load() (clusterpepper.Pepper, error) {
	plaintext, _, err := f.keys.readCredential(f.dataDir, clusterPepperCredential)
	if err != nil {
		return clusterpepper.Pepper{}, err
	}
	pepper, err := clusterpepper.DecodeFile(plaintext)
	if err != nil {
		return clusterpepper.Pepper{}, fmt.Errorf("%s is not a cluster password key: %w. %s",
			clusterPepperCredential.path(f.dataDir), err, clusterPepperCredential.restore)
	}
	return pepper, nil
}

// Store writes the pepper durably, sealed when the node seals its credentials.
func (f clusterPepperFile) Store(pepper clusterpepper.Pepper) error {
	return f.keys.writeCredential(f.dataDir, clusterPepperCredential, clusterpepper.EncodeFile(pepper))
}

// MoveAside keeps a wrong pepper as auth.cluster.pepper.wrong-<kid>; it never deletes it.
func (f clusterPepperFile) MoveAside(kid string) (string, error) {
	return clusterpepper.MoveAside(clusterPepperCredential.path(f.dataDir), kid)
}
