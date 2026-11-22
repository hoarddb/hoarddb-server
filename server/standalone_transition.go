package server

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// refuseStandaloneUsersInCluster refuses to start a clustered node on a data
// directory whose users hang off the standalone signing key, auth.token.key
// (BUG-0320). A clustered node signs with the key derived from the cluster
// token and never loads auth.token.key, so such users cannot be opened (a
// users.json sealed under that key) or cannot log in (passwords peppered
// under it). Converting them in place is not offered: the master would push
// users peppered under a key no other member holds. The owner's ruling
// (2026-10-18) is to refuse before anything is written and name the steps
// that do work, leaving the directory able to start standalone again.
//
// It runs only on the cluster path, after the keyring is built and before
// LoadUsers. A users.json it cannot read is left to LoadUsers, which reports
// that failure with its own error.
func refuseStandaloneUsersInCluster(dataDir string, keyring *Keyring, nodeMaster *NodeMaster, credentials CredentialKeys) error {
	keyBytes, _, err := credentials.readCredential(dataDir, authKeyCredential)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read auth signing key: %w", err)
	}
	standaloneKID := newSigningKey(keyBytes).ID

	usersPath := UsersFilePath(dataDir)
	usersBytes, err := os.ReadFile(usersPath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read users file %s: %w", usersPath, err)
	}
	if kid, legacy := legacyUsersFileKID(usersBytes); legacy && kid == standaloneKID {
		return standaloneDataDirError(dataDir, standaloneKID)
	}
	users, err := LoadUsersConfig(usersPath, keyring, nodeMaster)
	if err != nil {
		// LoadUsers runs next on the same file and reports this failure.
		return nil
	}
	for _, record := range users.Users {
		if record.Credentials.PepperKID == standaloneKID {
			return standaloneDataDirError(dataDir, standaloneKID)
		}
	}
	return nil
}

// legacyUsersFileKID returns the kid in the header of a users.json sealed
// under a signing key (layout version 0001), the layout decryptConfig reads.
func legacyUsersFileKID(usersBytes []byte) (string, bool) {
	if len(usersBytes) < usersHeaderLen || !isEncryptedUsersFile(usersBytes) {
		return "", false
	}
	versionStart := usersFileMagicLen
	kidStart := versionStart + usersFileVersionLen
	if !bytes.Equal(usersBytes[versionStart:kidStart], usersFileVersion) {
		return "", false
	}
	return string(usersBytes[kidStart : kidStart+usersFileKIDLen]), true
}

// standaloneDataDirError names the transition and the commands that move a
// standalone node's data and users into a cluster. dump does not carry users,
// so they are listed on the standalone node and created again.
func standaloneDataDirError(dataDir, standaloneKID string) error {
	return fmt.Errorf(`this data directory (%s) ran as a standalone node: its users are stored under its own signing key, auth.token.key (key %s),
       but a clustered node (HOARDDB_SEED_NODES set, or HOARDDB_REPLICATION_FACTOR above 1) signs with the key derived from the cluster token, so those users cannot be used in a cluster
       a standalone data directory cannot become a cluster member in place; nothing was changed, and it still starts without HOARDDB_SEED_NODES and HOARDDB_REPLICATION_FACTOR
       to move its data and users into the cluster:
         1. start this node again without HOARDDB_SEED_NODES and HOARDDB_REPLICATION_FACTOR, connect with hoarddb and note each user: user list, then user describe <name>
         2. dump its data: hoarddb dump --address <this node's client address> --user <admin> --out ./standalone-dump
         3. stop it, and start the cluster member on an empty data directory: a new HOARDDB_DATA_DIR, with HOARDDB_SEED_NODES set
         4. restore the data into the cluster: hoarddb restore --address <a member's client address> --user <admin> --dir ./standalone-dump
         5. on the cluster, create each user again with its roles: create user <name> roles [role@db, ...] (dump does not carry users or passwords)`,
		dataDir, standaloneKID)
}
